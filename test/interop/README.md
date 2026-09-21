# Reference SDK interop

The same seven scenarios run against the agent this suite serves, from
three independent A2A implementations over gRPC. None of them shares
code with `livery_grpc_a2a`, so agreement is evidence about the wire
rather than about our own client.

| SDK | Pinned in | Group | Toolchain |
|---|---|---|---|
| `a2a-sdk` (Python) | `requirements-a2a.txt` | `python` | `INTEROP_PYTHON`, or the venv |
| `a2a-go` | `go/go.mod` | `go` | `go/bin/client`, built |
| `@a2a-js/sdk` | `js/package.json` | `js` | `node` plus `js/node_modules` |

A group skips when its toolchain is absent, so `rebar3 ct` needs none
of them.

## Run

```sh
make interop-a2a   # Python
make interop-go    # Go
make interop-js    # JavaScript
make interop       # the whole suite, every group
```

## The contract

Each client takes `<host:port> <scenario>` and prints one JSON object
per line on stdout, ending when the scenario is done. The suite asserts
on those objects, so the step names and field names are the same in
every language. Exit code 0 means the scenario ran to the end.

| scenario | what it checks |
|---|---|
| `send` | blocking `SendMessage`; one task, already complete, carrying the echoed artifact |
| `stream` | `SendStreamingMessage`; event order and the artifact reassembled from two chunks |
| `multiturn` | `ask` pauses in `input_required`; a follow-up on the same task completes |
| `cancel` | return-immediately, then `CancelTask`, then `GetTask` reads `canceled` |
| `get` | `GetTask` after a completed send returns the same id and artifact |
| `direct` | `direct` answers with a Message rather than a Task |
| `error` | a missing task arrives as an error naming it, not as a success |

## Adding a language

1. Write a client under `test/interop/<lang>/` honouring the contract
   above. Mirror an existing one: the scenarios are small and the
   assertions are shared, so a field named differently is a failure.
2. Add a clause to `runner/1` in `livery_grpc_a2a_SUITE` and the group
   to `all/0` and `groups/0`.
3. Add `interop-<lang>` to the Makefile and a job to CI.

## Notes

- Versions are pinned. Bump them on purpose and run every group after.
- Two quirks worth knowing, both found by writing these clients: the
  JavaScript SDK serialises an `undefined` `taskId` literally, so a
  request must carry the proto default instead, and its non-streaming
  send may answer with the bare object rather than the oneof wrapper.
