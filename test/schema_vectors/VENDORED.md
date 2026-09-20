# A2A schema vectors

Vendored, not fetched. These are canonical instances of each A2A
`$defs` type, and the codec round trip in
`livery_grpc_a2a_codec_tests` runs every one of them through
`to_pb` and back.

- Source: [barrel_a2a](https://github.com/barrel-platform/barrel_a2a),
  `test/schema_vectors/1.0.1/examples`, at `1947a89` (v0.2.0).
- barrel_a2a builds them from the JSON examples in the A2A
  specification (sections 6 and 8.5), writing one from the proto
  definition where the specification has none.

They are copied here rather than read from the dependency because
barrel_a2a's hex package ships `src`, `priv`, `guides` and `docs`,
not `test`. Reading them from a sibling checkout worked on a
developer's machine and silently contributed nothing in CI, which is
the failure this copy removes: `vector_dir/0` now fails loudly rather
than reporting "vectors unavailable" and passing.

## Updating

Deliberate, never automatic:

```sh
cp -R ../barrel_a2a/test/schema_vectors/1.0.1/examples \
      test/schema_vectors/1.0.1/
```

Then run `rebar3 eunit`. A vector the codec cannot round trip is
either a codec bug or a type the binding does not carry; the second
needs a line in `skipped/0` with a reason, not a silent pass.
