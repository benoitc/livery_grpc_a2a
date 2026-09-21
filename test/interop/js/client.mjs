// A2A JavaScript SDK client over gRPC, driven by the CT suite.
//
// usage: client.mjs <host:port> <scenario>
//
// Builds an Agent Card naming a single GRPC interface at the given
// target, creates an SDK client bound to that binding and runs one
// scenario against the suite's agent. Every step prints one JSON object
// per line on stdout; the Erlang side asserts on those. Exit code 0
// means the scenario ran to the end.
//
// This mirrors test/interop/a2a_grpc_client.py and the Go client step
// for step and field for field: the suite runs the same assertions
// against every language.

import * as grpc from '@grpc/grpc-js';

import { A2A_PROTOCOL_VERSION, Role, TaskState } from '@a2a-js/sdk';
import { ClientFactory, ClientFactoryOptions } from '@a2a-js/sdk/client';
import { GrpcTransportFactory } from '@a2a-js/sdk/client/grpc';

const TIMEOUT_MS = 40000;

const emit = (fields) => process.stdout.write(`${JSON.stringify(fields)}\n`);

const stateName = (state) => TaskState[state] ?? String(state);

const partsText = (parts) =>
  (parts ?? [])
    .filter((p) => p.content?.$case === 'text')
    .map((p) => p.content.value)
    .join('');

const artifactText = (task) =>
  (task?.artifacts ?? []).map((a) => partsText(a.parts)).join('');

// A single GRPC interface at the target, which is what the suite serves.
const card = (target) => ({
  name: 'gRPC Test Agent',
  description: 'Card built by the interop client',
  version: '1.0.0',
  supportedInterfaces: [
    {
      url: target,
      protocolBinding: 'GRPC',
      tenant: '',
      protocolVersion: A2A_PROTOCOL_VERSION,
    },
  ],
  capabilities: {
    streaming: true,
    pushNotifications: false,
    extensions: [],
    extendedAgentCard: false,
  },
  securitySchemes: {},
  securityRequirements: [],
  defaultInputModes: ['text/plain'],
  defaultOutputModes: ['text/plain'],
  skills: [],
  documentationUrl: '',
  signatures: [],
});

async function newClient(target) {
  const options = ClientFactoryOptions.createFrom(ClientFactoryOptions.default, {
    transports: [
      new GrpcTransportFactory({
        grpcChannelCredentials: grpc.credentials.createInsecure(),
      }),
    ],
    preferredTransports: ['GRPC'],
  });
  return new ClientFactory(options).createFromAgentCard(card(target));
}

const message = (text, taskId, returnImmediately) => ({
  message: {
    role: Role.ROLE_USER,
    messageId: crypto.randomUUID(),
    parts: [
      {
        content: { $case: 'text', value: text },
        metadata: undefined,
        filename: '',
        mediaType: 'text/plain',
      },
    ],
    // Proto defaults, not `undefined': the SDK serialises an
    // `undefined' taskId literally and the server then looks for a task
    // called "undefined".
    taskId: taskId ?? '',
    contextId: '',
    extensions: [],
    metadata: {},
    referenceTaskIds: [],
  },
  ...(returnImmediately ? { configuration: { returnImmediately: true } } : {}),
});

const KINDS = {
  task: 'task',
  message: 'message',
  statusUpdate: 'status_update',
  artifactUpdate: 'artifact_update',
};

const kindOf = (payload) => KINDS[payload?.$case] ?? 'empty';

// A blocking send answers with one event; reported as a single-entry
// list so both shapes read the same.
async function send(client, request) {
  const res = await client.sendMessage(request);
  if (res?.payload?.$case) return [res.payload];
  // Some transports answer with the bare object rather than the oneof.
  return [res?.status ? { $case: 'task', value: res } : { $case: 'message', value: res }];
}

async function stream(client, request) {
  const events = [];
  for await (const ev of client.sendMessageStream(request)) {
    if (ev?.payload) events.push(ev.payload);
  }
  return events;
}

const kinds = (events) => events.map(kindOf);

function lastTask(events) {
  let task = null;
  for (const p of events) {
    if (p.$case === 'task') {
      task = p.value;
    } else if (p.$case === 'statusUpdate' && task) {
      task.status = p.value.status;
    } else if (p.$case === 'artifactUpdate' && task) {
      task.artifacts = [...(task.artifacts ?? []), p.value.artifact];
    }
  }
  return task;
}

const streamedText = (events) =>
  events
    .filter((p) => p.$case === 'artifactUpdate')
    .map((p) => partsText(p.value.artifact.parts))
    .join('');

const scenarios = {
  async send(client) {
    const events = await send(client, message('echo: interop'));
    const task = lastTask(events);
    emit({
      step: 'send',
      kinds: kinds(events),
      state: stateName(task.status.state),
      text: artifactText(task),
    });
  },

  async stream(client) {
    const events = await stream(client, message('stream'));
    emit({
      step: 'stream',
      kinds: kinds(events),
      states: events
        .filter((p) => p.$case === 'statusUpdate')
        .map((p) => stateName(p.value.status.state)),
      text: streamedText(events),
    });
  },

  async multiturn(client) {
    const task = lastTask(await send(client, message('ask')));
    emit({ step: 'ask', state: stateName(task.status.state), task_id: task.id });
    const done = lastTask(await send(client, message('second', task.id)));
    emit({
      step: 'follow_up',
      state: stateName(done.status.state),
      task_id: done.id,
      text: artifactText(done),
    });
  },

  async cancel(client) {
    const task = lastTask(await send(client, message('cancel-me', undefined, true)));
    emit({ step: 'started', task_id: task.id });
    const cancelled = await client.cancelTask({ id: task.id });
    emit({ step: 'cancel', state: stateName(cancelled.status.state) });
    const read = await client.getTask({ id: task.id });
    emit({ step: 'get', state: stateName(read.status.state) });
  },

  async get(client) {
    const task = lastTask(await send(client, message('echo: fetch me')));
    const read = await client.getTask({ id: task.id });
    emit({
      step: 'get',
      state: stateName(read.status.state),
      text: artifactText(read),
      same_id: read.id === task.id,
    });
  },

  async direct(client) {
    const events = await stream(client, message('direct'));
    emit({
      step: 'direct',
      kinds: kinds(events),
      text: events
        .filter((p) => p.$case === 'message')
        .map((p) => partsText(p.value.parts))
        .join(''),
    });
  },

  // A missing task must arrive as the A2A reason, not a bare status.
  // Every language reports the same UPPER_SNAKE reason from
  // google.rpc.ErrorInfo, which A2AError carries as `reason'.
  async error(client) {
    try {
      await client.getTask({ id: 'no-such-task' });
      emit({ step: 'error', error: 'none', text: '' });
    } catch (err) {
      emit({
        step: 'error',
        error: err?.reason ?? err?.constructor?.name ?? 'error',
        text: String(err?.message ?? err),
      });
    }
  },
};

async function main([target, name]) {
  if (!target || !scenarios[name]) {
    process.stderr.write('usage: client.mjs <host:port> <scenario>\n');
    process.exit(2);
  }
  const timer = setTimeout(() => {
    process.stderr.write(`timed out after ${TIMEOUT_MS} ms\n`);
    process.exit(1);
  }, TIMEOUT_MS);
  timer.unref?.();
  await scenarios[name](await newClient(target));
}

main(process.argv.slice(2)).then(
  () => process.exit(0),
  (err) => {
    process.stderr.write(`${err?.stack ?? err}\n`);
    process.exit(1);
  },
);
