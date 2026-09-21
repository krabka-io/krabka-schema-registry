# Confluent JVM serializer fixtures

`frames.txt` was captured on 2026-09-10 with the real Confluent serializers in
`mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`, image digest
`sha256:45894007bc1b54ae9db571328ec38d535b3a17e001a60956eb76f6a6212222d6`.
The harness uses `KafkaAvroSerializer`, `KafkaJsonSchemaSerializer`, and
`KafkaProtobufSerializer` against the Schema Registry REST service in that
container and a fresh in-process Krabka broker.

Regenerate the file from the repository root:

```sh
tools/capture-serde-fixtures.sh
```

The lane is serialized with the other fixed-port Docker suites. It selects the
named ignored capture test exactly and passes `--no-tests fail`, so a renamed or
missing capture test cannot produce a false green run. The five records cover
Avro, JSON Schema, Protobuf's optimized `[0]` message index, a nested `[1, 0]`
message index, and repeated/enum Protobuf fields. `jvm_fixtures.rs` asserts the
checked-in bytes against the Rust serializers without requiring Docker.
