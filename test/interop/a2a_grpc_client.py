"""A2A Python SDK client over gRPC, driven by the CT suite.

usage: a2a_grpc_client.py <host:port> <scenario>

Builds an Agent Card naming a single ``GRPC`` interface at the given
target, creates an SDK client bound to that binding and runs one scenario
against the suite's agent. Every step prints one JSON object per line on
stdout; the Erlang side asserts on those. Exit code 0 means the scenario
ran to the end.
"""

import asyncio
import json
import sys
import traceback
import uuid

import grpc

from a2a.client import ClientConfig, ClientFactory
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    CancelTaskRequest,
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskState,
)
from a2a.utils.constants import TransportProtocol
from a2a.utils.errors import A2A_ERROR_REASONS

TIMEOUT_SECONDS = 40


def emit(**fields):
    sys.stdout.write(json.dumps(fields) + "\n")
    sys.stdout.flush()


def card(target):
    return AgentCard(
        name="gRPC Test Agent",
        description="Agent under interop test",
        version="1.2.3",
        supported_interfaces=[
            AgentInterface(
                url=target,
                protocol_binding=TransportProtocol.GRPC,
                protocol_version="1.0",
            )
        ],
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
    )


def client_for(target, streaming=True):
    """A client bound to the GRPC binding.

    ``streaming`` picks which rpc the SDK uses for a send:
    SendStreamingMessage when true, SendMessage when false.
    """
    config = ClientConfig(
        streaming=streaming,
        polling=False,
        supported_protocol_bindings=[TransportProtocol.GRPC],
        grpc_channel_factory=lambda url: grpc.aio.insecure_channel(url),
    )
    return ClientFactory(config).create(card(target))


def message(text, task_id=None, return_immediately=False):
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=Role.ROLE_USER,
        parts=[Part(text=text)],
    )
    if task_id:
        msg.task_id = task_id
    request = SendMessageRequest(message=msg)
    if return_immediately:
        request.configuration.CopyFrom(
            SendMessageConfiguration(return_immediately=True)
        )
    return request


def artifact_text(task):
    return "".join(
        part.text for artifact in task.artifacts for part in artifact.parts if part.text
    )


def streamed_text(events):
    """Artifact text as it arrives in chunks on a stream."""
    return "".join(
        part.text
        for event in events
        if kind(event) == "artifact_update"
        for part in event.artifact_update.artifact.parts
        if part.text
    )


def final_state(events):
    """The state of the last event that carries one."""
    for event in reversed(events):
        if kind(event) == "status_update":
            return TaskState.Name(event.status_update.status.state)
        if kind(event) == "task":
            return TaskState.Name(event.task.status.state)
    return "NONE"


async def collect(client, request):
    """Drain send_message into a list of StreamResponse events."""
    events = []
    async for event in client.send_message(request):
        events.append(event)
    return events


def kind(event):
    return event.WhichOneof("payload")


def last_task(events):
    for event in reversed(events):
        if kind(event) == "task":
            return event.task
    return None


async def scenario_send(client):
    """Blocking SendMessage: one Task, already complete."""
    events = await collect(client, message("echo: interop"))
    task = last_task(events)
    emit(
        step="send",
        kinds=[kind(e) for e in events],
        state=TaskState.Name(task.status.state),
        text=artifact_text(task),
    )


async def scenario_stream(client):
    """SendStreamingMessage: the event order and the chunked artifact."""
    events = await collect(client, message("stream"))
    emit(
        step="stream",
        kinds=[kind(e) for e in events],
        states=[
            TaskState.Name(e.status_update.status.state)
            for e in events
            if kind(e) == "status_update"
        ],
        text=streamed_text(events),
    )


async def scenario_multiturn(client):
    events = await collect(client, message("ask"))
    task = last_task(events)
    emit(step="ask", state=final_state(events), task_id=task.id)
    events = await collect(client, message("second", task_id=task.id))
    done = last_task(events)
    emit(
        step="follow_up",
        state=TaskState.Name(done.status.state),
        task_id=done.id,
        text=artifact_text(done),
    )


async def scenario_cancel(client):
    events = await collect(client, message("cancel-me", return_immediately=True))
    task = last_task(events)
    emit(step="started", task_id=task.id)
    cancelled = await client.cancel_task(CancelTaskRequest(id=task.id))
    emit(step="cancel", state=TaskState.Name(cancelled.status.state))
    read = await client.get_task(GetTaskRequest(id=task.id))
    emit(step="get", state=TaskState.Name(read.status.state))


async def scenario_get(client):
    events = await collect(client, message("echo: fetch me"))
    task = last_task(events)
    read = await client.get_task(GetTaskRequest(id=task.id))
    emit(
        step="get",
        state=TaskState.Name(read.status.state),
        text=artifact_text(read),
        same_id=read.id == task.id,
    )


async def scenario_direct(client):
    events = await collect(client, message("direct"))
    replies = [e.message for e in events if kind(e) == "message"]
    emit(
        step="direct",
        kinds=[kind(e) for e in events],
        text="".join(part.text for m in replies for part in m.parts),
    )


async def scenario_error(client):
    """A missing task must arrive as the A2A reason, not a bare status.

    Every language reports the same UPPER_SNAKE reason from
    ``google.rpc.ErrorInfo`` so the suite can assert one value. The SDK
    raises a typed exception; ``A2A_ERROR_REASONS`` is its own mapping
    from that type back to the reason.
    """
    try:
        await client.get_task(GetTaskRequest(id="no-such-task"))
    except Exception as exc:  # noqa: BLE001 - the reason is what we report
        reason = A2A_ERROR_REASONS.get(type(exc), type(exc).__name__)
        emit(step="error", error=reason, text=str(exc))
        return
    emit(step="error", error="none", text="")


#: scenario -> (runner, streaming). A scenario that must see the task
#: pause (`ask`) or stay running (`cancel-me`) uses the blocking
#: SendMessage: a stream stays open until the task is final.
SCENARIOS = {
    "send": (scenario_send, False),
    "stream": (scenario_stream, True),
    "multiturn": (scenario_multiturn, False),
    "cancel": (scenario_cancel, False),
    "get": (scenario_get, False),
    "direct": (scenario_direct, True),
    "error": (scenario_error, False),
}


async def main():
    if len(sys.argv) != 3 or sys.argv[2] not in SCENARIOS:
        sys.stderr.write(__doc__)
        return 2
    target, name = sys.argv[1], sys.argv[2]
    run, streaming = SCENARIOS[name]
    client = client_for(target, streaming)
    try:
        await asyncio.wait_for(run(client), TIMEOUT_SECONDS)
    finally:
        await client.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:  # noqa: BLE001 - report everything to the CT log
        traceback.print_exc()
        sys.exit(1)
