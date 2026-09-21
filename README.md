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

## Install

Add the package to your `rebar.config`:

```erlang
{deps, [
    {livery_grpc_a2a, "~> 0.1.0"}
]}.
```

It brings `livery_grpc`, `livery` and `barrel_a2a` with it. The modules
use `-moduledoc`, so you need OTP 27 or later; CI runs OTP 28.

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

See the [A2A guide](docs/guides/a2a.md), and the
[changelog](CHANGELOG.md) for what each release carries.

## Tests

```sh
rebar3 eunit    # codec, including the schema vectors
rebar3 ct       # everything below, skipping what is not installed
make interop    # the reference SDKs: Python, Go and JavaScript
make check      # the lot, plus dialyzer, xref, elvis and erlfmt
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

**Interop** drives the same agent from three independent SDKs over
gRPC, none of which shares code with this one: `a2a-python`, `a2a-go`
and `@a2a-js/sdk`, seven scenarios each. Every group skips when its
toolchain is absent, so `rebar3 ct` never needs any of them. See
`test/interop/README.md`.

[`docs/compliance.md`](docs/compliance.md) maps every RPC, wire
requirement and error rule to the test that proves it.

## License

Apache 2.0.
