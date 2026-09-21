# Spec coverage

This page maps the gRPC binding of the A2A v1.0.1 specification
(section 10) to the test that proves it. Use it to check whether a
behaviour you rely on is covered before reading the code.

The binding carries no protocol of its own: `barrel_a2a` owns the
objects and the task lifecycle, `livery_grpc` owns the wire. What is
tested here is the conversion between them and the gRPC surface.

## Operations

Every RPC `A2AService` declares is served. `livery_grpc_a2a` maps each
one onto `barrel_a2a_server_core:call/4`, so the semantics behind them
are barrel_a2a's and are covered by its own suites.

| RPC | Kind | Covered by |
|---|---|---|
| `SendMessage` | unary | `t_send`, `t_send_direct_message`, `t_ref_send`, `t_wire_unary` |
| `SendStreamingMessage` | server stream | `t_streaming_send`, `t_ref_stream`, `t_wire_server_stream` |
| `GetTask` | unary | `t_ref_get`, `t_task_not_found` |
| `ListTasks` | unary | `t_list_tasks` |
| `CancelTask` | unary | `t_cancel`, `t_cancel_stream`, `t_ref_cancel` |
| `SubscribeToTask` | server stream | `t_subscribe_to_task` |
| `CreateTaskPushNotificationConfig` | unary | `t_push_config` |
| `GetTaskPushNotificationConfig` | unary | `t_push_config` |
| `ListTaskPushNotificationConfigs` | unary | `t_push_config` |
| `DeleteTaskPushNotificationConfig` | unary | `t_push_config` |
| `GetExtendedAgentCard` | unary | `t_extended_card` |

## Wire format

| Requirement | Covered by |
|---|---|
| ProtoJSON conversion in both directions loses nothing | `livery_grpc_a2a_codec_tests`, 120 schema vectors round tripped |
| What the codec writes matches the official A2A JSON Schema | the same vectors, validated with `barrel_a2a_schema:validate/2` |
| A request built by a foreign encoder is read correctly | the three reference SDK groups |
| Unary request and response framing | `t_wire_unary` |
| Server streaming framing and event order | `t_wire_server_stream`, `t_ref_stream` |
| An unknown method is `UNIMPLEMENTED` | `t_wire_unknown_method` |
| `grpc-timeout` is honoured | `t_wire_deadline` |
| Server reflection lists the service | `t_grpcurl_list` |

## Errors

An A2A error becomes a gRPC status through
`barrel_a2a_error:grpc_status/1`, with a `google.rpc.Status` in
`grpc-status-details-bin` whose first detail is an `ErrorInfo` carrying
the A2A reason and the `a2a-protocol.org` domain.

| Requirement | Covered by |
|---|---|
| The status code matches the A2A error type | `t_wire_error_details`, `t_task_not_found` |
| The A2A reason travels in `ErrorInfo` | `t_wire_error_details` |
| A foreign client reads the A2A reason back | `t_ref_error`, for each SDK |
| An unsupported protocol version is refused | `t_version_error` |
| The error is legible to a generic gRPC client | `t_grpcurl_error` |

The reason is asserted twice over, and both are the same string. On the
wire, `t_wire_error_details` decodes `grpc-status-details-bin` and reads
`ErrorInfo.reason` directly. Through the SDKs, each reference client
reports the reason its own library exposes, so `t_ref_error` asserts one
value for all three: `a2a-go` has `ErrorReason/1`, `a2a-js` carries it on
`A2AError.reason`, and `a2a-python` maps its exception type back through
`A2A_ERROR_REASONS`. An SDK that surfaced a bare gRPC status instead of
the A2A reason would fail that case.

## Interop

The same seven scenarios run against the served agent from three
independent implementations, none of which shares code with this one:

| SDK | Transport | Group |
|---|---|---|
| [`a2a-python`](https://github.com/a2aproject/a2a-python) | gRPC | `python` |
| [`a2a-go`](https://github.com/a2aproject/a2a-go) | gRPC | `go` |
| [`@a2a-js/sdk`](https://github.com/a2aproject/a2a-js) | gRPC (Node only) | `js` |

Each group skips when its toolchain is absent, so `rebar3 ct` needs
none of them. See `test/interop/README.md` for the client contract and
how to add a fourth language.

## Not covered here

The gRPC binding does not implement push notification delivery,
authentication or the agent card itself; those are barrel_a2a's and are
covered by its suites. Client streaming and bidirectional streaming are
not part of A2A: every streaming RPC in `A2AService` is server
streaming.

Server reflection cannot serve this binding's message schemas, because
gpb's descriptor output omits the synthetic map entry types a `map<>`
field needs. `t_grpcurl_list` covers listing the service; describing a
message is the gap, tracked in the README.
