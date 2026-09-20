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

## Known gap

Server reflection cannot serve this binding's message schemas. `a2a.proto`
uses `map<>` fields, and gpb's descriptor output omits the synthetic map
entry types they need (`google.protobuf.Struct.FieldsEntry` and friends),
which protoreflect rejects. The fix belongs in `livery_grpc_reflection`,
which is where the `FileDescriptorSet` is split.

## Tests

```sh
rebar3 eunit        # codec, including the schema vectors
rebar3 ct           # a served agent, driven over gRPC in both directions
make interop-a2a    # the same agent, driven by the official Python SDK
make check          # the lot, plus dialyzer, xref, elvis and erlfmt
```

**eunit** runs 120 vendored schema vectors, one canonical instance of
each A2A type, twice over: every vector round trips through `to_pb` and
back with nothing lost or invented, and the JSON the codec writes is
validated against the official A2A JSON Schema bundle that `barrel_a2a`
ships in `priv`. The vectors live in `test/schema_vectors`; see its
`VENDORED.md` for where they come from and how to update them.

**ct** starts a real agent, serves it over gRPC and drives it three
ways: through `barrel_a2a_client` over the gRPC transport, through a raw
`livery_grpc_client`, and through `grpcurl` against server reflection.

**make interop-a2a** drives the same agent with the official A2A Python
SDK over its gRPC transport, so the wire is read by an implementation
that shares no code with this one. That group skips when the venv is
absent, so `rebar3 ct` never needs Python.

`livery_grpc` is pinned by tag until 0.2.0 reaches hex; switch it to
`{livery_grpc, "~> 0.2.0"}` and drop the explicit `livery` entry then.

## License

Apache 2.0.
