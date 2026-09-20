# livery_grpc_a2a

The [Agent2Agent](https://a2a-protocol.org/) gRPC binding, built on
[livery_grpc](https://github.com/benoitc/livery_grpc) for the wire and
[barrel_a2a](https://github.com/barrel-platform/barrel_a2a) for the
protocol. Serve an A2A agent over gRPC, or talk to one.

It adds no transport and no protocol of its own. `livery_grpc` carries
the gRPC framing; `barrel_a2a` owns the objects, the task lifecycle and
the binding-neutral engine. This package is the adapter between them:
`a2a.proto` messages in, `barrel_a2a_server_core:call/4` out.

## Why a separate package

A gRPC user should not take on an A2A implementation, and an A2A user
should not take on `gpb` and a code generation step. barrel_a2a's
decision record
[0005](https://github.com/barrel-platform/barrel_a2a/blob/main/docs/decisions/0005-grpc-in-a-separate-package.md)
sets out the split and the two contracts it rests on: the
binding-neutral `barrel_a2a_server_core:call/4`, and the
`barrel_a2a_client_transport` behaviour for the client side.

## Serve an agent

```erlang
{ok, Server} = barrel_a2a_server:start(Card, #{
    handler => my_agent,
    listen => false            %% gRPC is the transport here
}),
{ok, _} = livery_grpc:start_server(#{
    port => 50051,
    services => [livery_grpc_a2a:service(Server)]
}).
```

## Talk to an agent

```erlang
{ok, Agent} = barrel_a2a_client:connect(<<"http://127.0.0.1:50051">>, #{
    transports => [{<<"GRPC">>, livery_grpc_a2a_client}],
    prefer => [grpc]
}),
{ok, {task, Task}} = barrel_a2a_client:send(Agent, <<"hello">>).
```

See `docs/guides/a2a.md`.

## Status

Requires an unreleased `livery_grpc`: the binding needs the `config`
field of `livery_grpc:service_spec()`, which is not in 0.1.2. Until
that ships, build against a local checkout:

```sh
mkdir -p _checkouts
ln -s ../../livery_grpc _checkouts/livery_grpc
ln -s ../../livery      _checkouts/livery
rebar3 ct
```

## Known gap

Server reflection cannot serve this binding's message schemas. `a2a.proto`
uses `map<>` fields, and gpb's descriptor output omits the synthetic map
entry types they need (`google.protobuf.Struct.FieldsEntry` and friends),
which protoreflect rejects. The fix belongs in `livery_grpc_reflection`,
which is where the `FileDescriptorSet` is split.

## Tests

```sh
rebar3 eunit    # codec round trips
rebar3 ct       # a served agent, driven over gRPC in both directions
```

The CT suite also has a Python group that drives the server with the
official A2A gRPC client; it skips unless that environment is set up.

## License

Apache 2.0.
