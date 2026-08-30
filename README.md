# krabka-schema-registry

A Confluent Schema Registry-compatible service for
[krabka](https://github.com/krabka-io), written in Rust, together with the
client-side serdes that speak its wire format.

The registry is a Kafka *client*, not a broker component. It keeps every schema
in the `_schemas` compacted topic, so a krabka cluster is the only storage it
needs.

It builds on three sibling repositories and nothing else in the stack:
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for the wire
layer, [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) for
the Kafka client, and
[`krabka-broker`](https://github.com/krabka-io/krabka-broker) for the shared
authorizer and telemetry crates, and for the in-process broker the integration
suites run against.

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-schema-registry` | The service: the Confluent REST API, the `_schemas` store, primary election, compatibility checking, auth and ACLs. Ships three binaries. |
| `krabka-schema-serde` | The client side: the `0x00 \| id \| body` framing, the REST client, and the Avro, Protobuf and JSON-Schema serdes. |

### Binaries

| Binary | What it does |
| --- | --- |
| `krabka-schema-registry` | The registry server. |
| `krabka-schema-push` | Registers a schema file under a subject. |
| `krabka-schema-compat-check` | Checks a schema against a subject's history without registering it. |

## What it covers

- The Confluent REST API: `/subjects`, `/schemas`, `/config`, `/mode`, and the
  compatibility endpoints, in the shapes the JVM clients and `curl` recipes
  expect.
- **Avro, Protobuf and JSON Schema**, each with its own compatibility engine.
  All eight Confluent compatibility levels are supported.
- **Schema references**, so one schema can name another.
- **High availability.** Nodes join the cp-exact `"sr"` Kafka group, the group
  leader selects a primary, and a secondary forwards every write to it.
  Read-your-writes holds because a write waits for its own record to come back
  through the `_schemas` reader.
- **`IMPORT` mode**, for loading an existing registry's records with their ids
  and versions preserved.
- **Security.** TLS, HTTP basic auth, and Kafka-ACL authorization over the
  registry's own resources.

## Build

Bazel is the build and test path. Cargo stays the dependency source of truth:
[`rules_rs`](https://github.com/hermeticbuild/rules_rs) reads the same
`Cargo.toml` and `Cargo.lock` that Cargo does, so there is no second dependency
set to keep in sync.

```
bazel test //...
```

`cargo` works the same way:

```
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Clippy is a Cargo-side gate. `bazel build` applies `-Funsafe_code`, the one
`[workspace.lints]` entry whose guarantee must not lapse under a second build
system, but it does not run Clippy.

Nine capture suites are `#[ignore]`d. They drive a real
`confluentinc/cp-schema-registry` container to regenerate the golden fixtures
under `crates/schema-registry/tests/fixtures/`, so they need a Docker daemon and
they bind fixed ports. `.config/nextest.toml` keeps them from running
concurrently.

## Sibling revisions

Member manifests declare the sibling crates as ordinary
`krabka-x = "0.4.0"` requirements. The `[patch.crates-io]` block at the bottom
of the root `Cargo.toml` is the single place a sibling revision moves. To take a
newer sibling, change the revision there, re-run `cargo generate-lockfile`, and
commit both files.

`MODULE.bazel` additionally names each sibling crate's directory. rules_rs finds
a git crate's path by matching the crate name against the workspace `members`
list, and every sibling's `members` is the glob `crates/*`, which it skips.

## Documentation

- [Roadmap](docs/roadmap.md)
- [Deployment](docs/deploy.md)
- [Design documents](docs/design/)
- [Style guides](docs/style_guides/README.md)

## Not yet here

The Helm chart, the apko image definition and the operator's `SchemaRegistry`
CRD still live in [`robot-head/crabka`](https://github.com/robot-head/crabka).
The CRD belongs to that repository's operator crate, which is not moving, so the
packaging follows it rather than being split in half.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
