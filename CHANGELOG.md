# Changelog

All notable changes to this project are documented here. The format is
based on Keep a Changelog, and this project adheres to Semantic
Versioning.

## [0.1.0] - 2026-09-21

First release. The A2A gRPC binding moves out of `livery_grpc` into its
own package, so a gRPC user does not take on an A2A implementation and
an A2A user does not take on `gpb`.

### Added
- `livery_grpc_a2a:service/1,2` turns a running `barrel_a2a_server` into
  a service registration for `livery_grpc:start_server/1`. All eleven
  RPCs of `lf.a2a.v1.A2AService` are served through
  `barrel_a2a_server_core:call/4`, so authentication, version and
  extension negotiation, validation and the task lifecycle match the
  HTTP bindings.
- `livery_grpc_a2a_client`, a `barrel_a2a_client_transport` for the
  `GRPC` binding: unary calls, server streaming, stream cancel, and
  `timeout` sent as `grpc-timeout`.
- `livery_grpc_a2a_codec` converts between `a2a.proto` messages and
  canonical ProtoJSON in both directions.
- A2A errors map to a gRPC status plus a `google.rpc.Status` in
  `grpc-status-details-bin`, with an `ErrorInfo` carrying the A2A reason
  and a `BadRequest` for `invalid_params`.
- Tests: 120 vendored schema vectors validated against the official A2A
  JSON Schema, an end to end suite over `barrel_a2a_client`, a raw
  `livery_grpc_client` and grpcurl, and interop groups for the
  `a2a-python`, `a2a-go` and `@a2a-js/sdk` reference SDKs.
- `docs/guides/a2a.md` and `docs/compliance.md`, which maps each RPC,
  wire requirement and error rule to the test that proves it.

### Dependencies
- `livery_grpc` `~> 0.2.3`, the first hex package that declares `livery`
  and `h2`, and `barrel_a2a` `~> 0.2.0`.

### Known issues
- Server reflection lists the service but cannot serve its message
  schemas: gpb's descriptor output omits the map entry types that
  `map<>` fields need. Pass `-proto` to grpcurl.
- Trailing metadata does not echo the active extensions.
