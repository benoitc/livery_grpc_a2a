# A2A over gRPC

`livery_grpc_a2a` serves the
[A2A protocol](https://a2a-protocol.org) gRPC binding (specification
section 10) from a [barrel_a2a](https://github.com/barrel-platform/barrel_a2a)
agent, and `livery_grpc_a2a_client` talks to any A2A agent that offers a
`GRPC` interface. You need this when an agent has to answer over gRPC as
well as (or instead of) JSON-RPC and HTTP+JSON: same agent, same tasks,
same errors, a different wire.

Both sides go through barrel_a2a's binding-neutral core, so
authentication, version and extension negotiation, validation and the
task lifecycle behave exactly as they do over HTTP.

## Serve an agent over gRPC

Start the agent without a listener, then hand `service/1` to
`livery_grpc:start_server/1`.

```erlang
Card = barrel_a2a_agent_card:new(#{
    name    => <<"Recipe Agent">>,
    version => <<"1.0.0">>,
    supported_interfaces => [
        barrel_a2a_agent_card:interface(
            <<"https://agent.example:50051">>, <<"GRPC">>, <<"1.0">>
        )
    ],
    skills => [#{id => <<"suggest">>, name => <<"Suggest">>, tags => [<<"food">>]}]
}),

{ok, Agent} = barrel_a2a_server:start(Card, #{
    handler => my_agent,
    listen  => false
}),

{ok, Server} = livery_grpc:start_server(#{
    port     => 50051,
    services => [livery_grpc_a2a:service(Agent)]
}).
```

`my_agent` is an ordinary `barrel_a2a_handler`; nothing in it is gRPC
aware. `service/1` carries the agent pid in the registration's `config`,
so several agents can run in one node, each on its own gRPC listener.

Stop in the reverse order:

```erlang
ok = livery_grpc:stop_server(Server),
ok = barrel_a2a_server:stop(Agent).
```

## Add the interface to the Agent Card

A client picks a binding from the card's `supportedInterfaces`
(specification 8.3.2), so the card must name the gRPC endpoint. Build the
entry with `barrel_a2a_agent_card:interface/3`:

```erlang
Grpc = barrel_a2a_agent_card:interface(<<"https://agent.example:50051">>, <<"GRPC">>, <<"1.0">>).
```

Add a `tenant` with `interface/4` when one endpoint serves several
agents; clients then send it in every request's `tenant` field.

The URL gives the scheme, host and port. Its path is ignored: gRPC routes
on the fully qualified method name (`/lf.a2a.v1.A2AService/SendMessage`),
not on a base path.

## Share one agent between HTTP and gRPC

An agent can offer both bindings at once. Declare both interfaces on the
card and mount both front doors on the one `barrel_a2a_server`:

```erlang
Card = barrel_a2a_agent_card:new(#{
    name    => <<"Recipe Agent">>,
    version => <<"1.0.0">>,
    supported_interfaces => [
        barrel_a2a_agent_card:interface(<<"https://agent.example/a2a">>, <<"JSONRPC">>, <<"1.0">>),
        barrel_a2a_agent_card:interface(<<"https://agent.example:50051">>, <<"GRPC">>, <<"1.0">>)
    ],
    skills => [...]
}),

%% One agent, its own HTTP listener plus a gRPC listener.
{ok, Agent} = barrel_a2a_server:start(Card, #{
    handler => my_agent,
    http    => #{port => 8080}
}),
{ok, Server} = livery_grpc:start_server(#{
    port     => 50051,
    services => [livery_grpc_a2a:service(Agent)]
}).
```

Tasks, push notification configs and the auth hook are the agent's, not
the binding's, so a task started over JSON-RPC is visible over gRPC and
the reverse. With `listen => false` the agent has no listener of its own
and you mount its routes yourself (see barrel_a2a's embedding guide);
`service/1` works the same either way.

## Call an agent over gRPC

Register the transport for the `GRPC` binding and use `barrel_a2a_client`
as usual:

```erlang
{ok, Agent} = barrel_a2a_client:connect(<<"https://agent.example">>, #{
    transports => [{<<"GRPC">>, livery_grpc_a2a_client}],
    prefer     => [grpc, jsonrpc]
}),

{ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"suggest a soup">>),
ok = barrel_a2a_client:close(Agent).
```

`connect/2` fetches the card over HTTP, then opens the binding the card
and your `prefer` list agree on. When you already hold the card, skip the
fetch:

```erlang
{ok, Agent} = barrel_a2a_client:from_card(Card, #{
    transports => [{<<"GRPC">>, livery_grpc_a2a_client}],
    prefer     => [grpc]
}).
```

Streaming is unchanged too:

```erlang
{ok, Task} = barrel_a2a_client:start(Agent, <<"write the report">>),
ok = barrel_a2a_remote_task:stream_to(Task, self()),
receive
    {a2a_event, Task, Event} -> handle(Event);
    {a2a_done, Task, Final}  -> Final
end.
```

## Errors

An A2A error becomes the gRPC status `barrel_a2a_error:grpc_status/1`
maps it to, plus a `google.rpc.Status` in `grpc-status-details-bin` whose
first detail is an `ErrorInfo` carrying the A2A reason and the
`a2a-protocol.org` domain. That is what lets a client recover the exact
error type when several types share one status:

```erlang
{error, Error} = barrel_a2a_client:get_task(Agent, <<"no-such-task">>),
task_not_found = barrel_a2a_error:type(Error).
```

| A2A error | gRPC status | `ErrorInfo.reason` |
|---|---|---|
| `task_not_found` | `NOT_FOUND` | `TASK_NOT_FOUND` |
| `task_not_cancelable` | `FAILED_PRECONDITION` | `TASK_NOT_CANCELABLE` |
| `unsupported_operation` | `FAILED_PRECONDITION` | `UNSUPPORTED_OPERATION` |
| `version_not_supported` | `FAILED_PRECONDITION` | `VERSION_NOT_SUPPORTED` |
| `invalid_params` | `INVALID_ARGUMENT` | `INVALID_PARAMS` |
| `unauthenticated` | `UNAUTHENTICATED` | `UNAUTHENTICATED` |
| `rate_limited` | `RESOURCE_EXHAUSTED` | `RATE_LIMITED` |

An `invalid_params` error also carries a `BadRequest` detail naming the
field. Any other language's A2A client reads both details: the official
Python SDK, for instance, raises `TaskNotFoundError` rather than a bare
`NOT_FOUND`.

## Metadata, versions and deadlines

- `a2a-version` and `a2a-extensions` travel as call metadata and reach
  the engine's negotiation exactly as the HTTP headers of the same name
  do. A request without `a2a-version` is treated as the legacy version,
  which a `1.0` server rejects unless it was started with
  `accept_legacy_version => true`; `barrel_a2a_client` always sends it.
- `authorization` reaches barrel_a2a's own auth hook unchanged. When
  livery middleware has already authenticated the caller
  (`livery_ext:user/2`), that principal is passed instead and the hook is
  skipped.
- A client `timeout` becomes `grpc-timeout`, so the agent enforces the
  same deadline. A stream that produces no event within the timeout ends
  with `DEADLINE_EXCEEDED`.

## Talking to the service directly

Start the server with `reflection => true` and grpcurl needs no proto
file:

```sh
grpcurl -plaintext localhost:50051 describe lf.a2a.v1.Task

grpcurl -plaintext \
  -H 'a2a-version: 1.0' \
  -d '{"message":{"messageId":"1","role":"ROLE_USER","parts":[{"text":"hello"}]}}' \
  localhost:50051 lf.a2a.v1.A2AService/SendMessage
```

Without reflection, point at the vendored proto:

```sh
grpcurl -plaintext -import-path proto -proto a2a.proto \
  -H 'a2a-version: 1.0' \
  -d '{"message":{"messageId":"1","role":"ROLE_USER","parts":[{"text":"hello"}]}}' \
  localhost:50051 lf.a2a.v1.A2AService/SendMessage
```

Add `-proto google/rpc/error_details.proto` to have grpcurl decode the
`ErrorInfo` in a failure's details.

Reflection of `map<>` fields needs `livery_grpc` 0.2.4 or later, which
this package requires.

## Notes

- The JSON the codec exchanges with the engine is canonical ProtoJSON,
  so a field holding its type default is absent rather than written:
  `append` and `lastChunk` are missing from an artifact update rather
  than `false`. Read them with a default. Repeated fields are the one
  deliberate exception, always written (as `[]` when empty), because the
  A2A schema marks several arrays required.
- Timestamps carry millisecond precision, like every other barrel_a2a
  binding. A finer input precision is truncated, not rejected.
- A stream stays open until the task is final. An interrupted state
  (`input_required`, `auth_required`) does not close it, matching the
  SSE binding; use the blocking `SendMessage` when you want the call to
  return at that point.
- Trailing metadata does not echo the active extensions: livery_grpc has
  no way yet for a handler to add response metadata.
- `test/livery_grpc_a2a_SUITE` drives all of this three ways: through
  `barrel_a2a_client`, through a raw `livery_grpc_client` that pins the
  wire shape, and through grpcurl. `make interop` adds the `a2a-python`,
  `a2a-go` and `@a2a-js/sdk` reference SDKs, each over its own gRPC
  transport. See [Spec coverage](../compliance.md) for the full map.
