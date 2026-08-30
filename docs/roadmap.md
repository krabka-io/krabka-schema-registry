# krabka-schema-registry Roadmap

This roadmap lists the work that closes the remaining gaps between this registry and Confluent Schema Registry 7.4.0. Every item below was verified against the code before it was written down.

## Where the project stands

The registry is substantially complete. It serves nineteen REST routes with the Confluent error envelope and the `application/vnd.schemaregistry.v1+json` content type. It implements all seven compatibility levels across three format engines, and 201 verdicts captured from a real `cp-schema-registry` container gate them. The `_schemas` store gives read-your-writes barriers and fences its transactional writes. Primary election uses the cp-exact `"sr"` group protocol, pinned to captured bytes. Authentication accepts mTLS, Bearer and Basic credentials, and authorization reads ACLs from the brokers. The client crate `krabka-schema-serde` implements the wire framing, and it includes Confluent's single-byte message-index form.

Six domain surveys of the workspace found no missing subsystem. They found one pattern instead. The code paths that the golden fixtures cover are correct. The paths next to those fixtures have moved away from Confluent.

That pattern has three shapes.

Some rules were added in one place and not in the others. Three live routes skip the authorization gate, because `authz_target` has no arm for them. A client-settable header turns off both authentication and authorization on the only listener.

Some behavior is recorded in a fixture that no test reads. Four of six fixture sets are opened only by the `#[ignore]`d capture harnesses, and four other captures were copied by hand into string literals under `src/`. The recorded cp error strings already differ from the strings `SrError` renders.

Some engines are correct exactly as far as the captured matrix reaches. An Avro record that holds a decimal field cannot evolve at all. A change inside an imported `.proto` passes silently. A JSON subschema moved into `$defs` is reported incompatible. A registration after a hard delete re-issues a live version number, and the record key of that version destroys the older one under compaction.

The seven milestones are ordered by how much damage each defect does and by what each milestone gives the next one.

| Milestone | Theme | Issues |
| --- | --- | --- |
| M1 | Confluent-shaped errors and a closed authorization surface | 6 |
| M2 | `_schemas` store correctness | 4 |
| M3 | Avro and Protobuf compatibility parity | 4 |
| M4 | JSON Schema diff parity | 4 |
| M5 | REST surface completeness and fixture enforcement | 4 |
| M6 | Client serde parity | 5 |
| M7 | Build gates, interop proof and operability | 5 |

The end state is a registry where executable fixtures hold the Confluent contract at every layer. No route reaches a handler without an authorization decision. No captured byte string exists as a hand-copied literal. No compatibility rule differs from cp without a recorded divergence. The `_schemas` guarantee is proved in both directions: cp reads what krabka wrote, and krabka reads what cp wrote.

## M1 — Confluent-shaped errors and a closed authorization surface

**Theme.** Every routed endpoint passes through the ACL gate, and every response a client can provoke carries the Confluent error envelope.

**Why here.** These are the failures that a Confluent client and a security review both meet on the first request, and they are cheap to fix. Three registered routes skip authorization. A client-settable header turns off authentication and authorization outright. Several response paths send plain text where a JVM client expects `{"error_code": N}`. The error envelope work also gives every later milestone one place to render the new codes 50002, 50003, 50004 and 40301, so it unblocks the store and REST work that follows.

**Exit criteria.**

- Every path and method pair registered in `crates/schema-registry/src/rest/mod.rs:96-152` either maps to an authz target or appears, with a reason, in the unmapped-route test.
- No error response from the service, from a handler, middleware, the router fallback or an extractor rejection, has a body that fails to parse as `{"error_code": N, "message": "..."}`. A success response keeps its Confluent body shape. Every response, of either kind, carries `application/vnd.schemaregistry.v1+json`.
- A request that forges the inter-node forwarding header against a node started with `--require-auth --authz` is rejected.
- `cargo test -p krabka-schema-registry --test security` passes with the new deny cases.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#3](https://github.com/krabka-io/krabka-schema-registry/issues/3) authz: map DELETE /config/{subject} and the id-scoped reads to ACL targets | security | S | critical |
| [#4](https://github.com/krabka-io/krabka-schema-registry/issues/4) auth: authenticate the inter-node forward hop | security | M | critical |
| [#5](https://github.com/krabka-io/krabka-schema-registry/issues/5) rest: return the Confluent error envelope from every response path | rest | M | high |
| [#6](https://github.com/krabka-io/krabka-schema-registry/issues/6) serve: use real peer addresses and bind the admin listener to loopback | security | S | high |
| [#7](https://github.com/krabka-io/krabka-schema-registry/issues/7) auth: parse cp password.properties, honor roles, keep credentials off argv | security | S | high |
| [#8](https://github.com/krabka-io/krabka-schema-registry/issues/8) audit: emit events for authentication failures and authorization decisions | security | M | high |

## M2 — _schemas store correctness

**Theme.** The log is the database. The id and version bindings must be authoritative, the reader must survive a real cluster, and a write must never hang without a bound.

**Why here.** These defects corrupt data rather than return a wrong status, and several of them destroy history under compaction with no way back. They come second only because the error envelope from M1 gives them somewhere to report 50002 and 50004. Every layer above the store, meaning compatibility checks, REST reads and client caches, is built on the store's answers.

**Exit criteria.**

- A live store and a store rebuilt by a replay of the same `_schemas` log produce identical subject, version, id, config and mode state after a lifecycle that includes soft deletes, hard deletes, re-registration and IMPORT.
- A registry started against a pre-existing `_schemas` with the wrong partition count or cleanup policy refuses to start with a named error instead of a hang.
- No code path in `crates/schema-registry/src/kafkastore` awaits without a bound.
- A registration after a permanent delete never reuses a live version number, proved by an integration test.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#9](https://github.com/krabka-io/krabka-schema-registry/issues/9) store: make id and version bindings authoritative across register and delete | store | L | critical |
| [#10](https://github.com/krabka-io/krabka-schema-registry/issues/10) kafkastore: make the _schemas reader survive a real cluster | store | L | critical |
| [#11](https://github.com/krabka-io/krabka-schema-registry/issues/11) kafkastore: bound the store wait and fence before the barrier | store | M | high |
| [#12](https://github.com/krabka-io/krabka-schema-registry/issues/12) store: prove replay equivalence with a hermetic restart test | store | M | high |

## M3 — Avro and Protobuf compatibility parity

**Theme.** Bring the two format engines onto cp's rules for the cases that the 201 captured verdicts never reached.

**Why here.** A compatibility verdict is the registry's most consequential output. A false accept puts an incompatible schema into a subject's history forever. A false reject blocks the evolution paths that Confluent's own documentation recommends. The engine skeleton, the level resolution and the direction handling are already correct and the golden matrix gates them, so this milestone is contained rule work inside the format modules. It follows the store work because a wrong verdict over a correct store can be repaired, and a corrupted log cannot.

**Exit criteria.**

- `tests/compat_conformance.rs` still passes with an empty `known_divergences()`.
- Each rule added here has a matrix case captured from cp, not a hand-written expectation.
- A decimal-bearing Avro schema, an aliased record rename and an enum with a reader default all evolve successfully.
- A change inside an imported `.proto` is reported at the field site instead of accepted silently.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#13](https://github.com/krabka-io/krabka-schema-registry/issues/13) avro: pre-process schemas before can_read for logical types, defaults, aliases | compat | M | critical |
| [#14](https://github.com/krabka-io/krabka-schema-registry/issues/14) avro: stop dedup'ing schemas by Parsing Canonical Form | compat | M | high |
| [#15](https://github.com/krabka-io/krabka-schema-registry/issues/15) protobuf: diff schema references so a changed import is visible | compat | L | critical |
| [#16](https://github.com/krabka-io/krabka-schema-registry/issues/16) protobuf: classify oneof membership, required fields and explicit labels | compat | M | high |

## M4 — JSON Schema diff parity

**Theme.** Bring the JSON engine onto cp's `SchemaDiff` semantics. Resolve references first, model the content model per side, and diff the keywords that the engine ignores today.

**Why here.** The JSON engine has the widest distance between what it appears to cover and what it decides, because the 92 captured verdicts happen to exercise only same-shape pairs. Every finding here was reproduced by a run of the real engine and cross-read against Confluent's `json/diff/*.java`. The findings split into five independent areas of `diff.rs`, so the work runs in parallel. It follows M3 because the Avro and Protobuf defects include silent accepts on schema references, which are the same class of defect and rank higher.

**Exit criteria.**

- Every new rule is backed by a matrix case captured from cp 7.4.0, and `known_divergences()` stays empty.
- A subschema moved into `$defs` is compatible in both directions.
- No keyword in cp's `COMPATIBLE_CHANGES` and `INCOMPATIBLE` table is ignored silently by `compare_schema`.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#17](https://github.com/krabka-io/krabka-schema-registry/issues/17) json: resolve $ref before diffing a node, not after | compat | M | high |
| [#18](https://github.com/krabka-io/krabka-schema-registry/issues/18) json: model the object content model per side | compat | M | high |
| [#19](https://github.com/krabka-io/krabka-schema-registry/issues/19) json: diff combinators the way cp does | compat | L | high |
| [#20](https://github.com/krabka-io/krabka-schema-registry/issues/20) json: fix dependencies, tuple items, uniqueItems and numeric relations | compat | M | medium |

## M5 — REST surface completeness and fixture enforcement

**Theme.** Make the golden fixtures executable, and close the endpoint and parameter gaps that a Confluent client reaches for.

**Why here.** `CLAUDE.md` states that the fixtures are the specification, and four of six fixture sets are never opened by a test. A spot check shows that the implementation already differs from the recorded cp error strings. The fixture gate comes first inside this milestone, because it catches the drift that the rest of the milestone would otherwise introduce. The endpoint gaps that follow are the ones a Confluent JVM client hits without an explicit call: `testCompatibility` without a version, `defaultToGlobal` on a subject's config, and a lookup during a leader election.

**Exit criteria.**

- Every file under `crates/schema-registry/tests/fixtures/` is read by at least one non-Docker test.
- No captured cp byte string is duplicated as an inline literal in `crates/schema-registry/src`.
- `SchemaRegistryClient.testCompatibility(subject, schema)` returns a verdict instead of a routing failure.
- A lookup and a compatibility check against a secondary with no elected primary return 200.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#21](https://github.com/krabka-io/krabka-schema-registry/issues/21) tests: replay every golden fixture set and reconcile the drifted messages | rest | M | critical |
| [#22](https://github.com/krabka-io/krabka-schema-registry/issues/22) rest: add POST /compatibility/subjects/{subject}/versions | rest | M | high |
| [#23](https://github.com/krabka-io/krabka-schema-registry/issues/23) rest: complete the config, mode and listing query surface | rest | M | medium |
| [#24](https://github.com/krabka-io/krabka-schema-registry/issues/24) rest: serve read-only POSTs locally instead of forwarding them | rest | S | medium |

## M6 — Client serde parity

**Theme.** Make `krabka-schema-serde` produce and consume the bytes and schemas that a Confluent client produces and consumes, and prove it against captured cp output.

**Why here.** Every producer and consumer links this crate, and it is the only part of the repository with no integration tests and no captured cp evidence. Its wire framing is checked against its own encoder alone. The defects here corrupt data silently inside somebody else's application: a `.proto` that misstates cardinality, a message index that points at the wrong type, and a `UseLatest` mode that frames a record with an id whose schema does not describe it. It follows the server milestones because the registry's own normalizer, which the serde should reuse, is corrected there.

**Exit criteria.**

- Bytes that this crate produces for a flat message, a nested message, a repeated and enum message and an Avro record match captured JVM serializer output byte for byte.
- A `.proto` rendered by this crate for a schema with enums, nested messages, oneofs, maps and repeated fields re-parses into an equivalent `DescriptorPool`.
- A referenced Avro or JSON schema fetched from the registry decodes without a network call.
- `crates/schema-serde/tests/` exists and runs under `bazel test //...`.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#25](https://github.com/krabka-io/krabka-schema-registry/issues/25) serde: derive .proto text and message-index paths correctly | serde | M | critical |
| [#26](https://github.com/krabka-io/krabka-schema-registry/issues/26) serde: make UseLatest serialize against the registry's latest schema | serde | M | critical |
| [#27](https://github.com/krabka-io/krabka-schema-registry/issues/27) serde: carry schema references through register, lookup and decode | serde | M | high |
| [#28](https://github.com/krabka-io/krabka-schema-registry/issues/28) serde: surface the Confluent error_code as a typed error | serde | S | high |
| [#29](https://github.com/krabka-io/krabka-schema-registry/issues/29) serde: add cp golden byte fixtures and an integration suite | serde | L | high |

## M7 — Build gates, interop proof and operability

**Theme.** Make CI enforce what the configuration files already claim, prove the `_schemas` guarantee in both directions, and give a deployed node probes, metrics and a graceful exit.

**Why here.** This milestone protects what the first six deliver. The nine suites that talk to a real `cp-schema-registry` are not merely unrun. The `manual` tag keeps them out of `//...` for `build` as well as `test`, so a rename rots the fixture-regeneration path with no signal. The pedantic lint policy that `CLAUDE.md` forbids anyone to suppress is enforced in no CI job. `deny.toml` describes another repository's dependency graph. The milestone comes last because each earlier milestone adds the fixtures and rules that these gates then hold in place, and because operability work pays off once the behavior under it is correct.

**Exit criteria.**

- The capture and interop suites compile on every pull request and run on a scheduled Docker job that fails when a capture rewrites a committed fixture.
- `cargo-deny` and a pedantic Clippy run are both CI gates.
- A real `cp-schema-registry` boots against a `_schemas` topic that krabka wrote and serves the same ids, versions and subjects.
- A SIGTERM to a running node drains in-flight requests and leaves the `sr` group before the process exits.
- `/metrics` and `/readyz` answer on the admin listener and report reader lag.

| Issue | Area | Size | Value |
| --- | --- | --- | --- |
| [#30](https://github.com/krabka-io/krabka-schema-registry/issues/30) ci: enforce the Docker suites, the pedantic lint policy and cargo-deny | ops | M | critical |
| [#31](https://github.com/krabka-io/krabka-schema-registry/issues/31) interop: prove cp can read a _schemas topic krabka wrote | ops | M | high |
| [#32](https://github.com/krabka-io/krabka-schema-registry/issues/32) ops: serve metrics and readiness on the admin listener | ops | M | high |
| [#33](https://github.com/krabka-io/krabka-schema-registry/issues/33) ops: handle SIGTERM, drain TLS connections and bound the listeners | ops | S | high |
| [#34](https://github.com/krabka-io/krabka-schema-registry/issues/34) docs: rewrite deploy.md and correct the stale API documentation | ops | S | medium |

## What is not on the roadmap

**Backwards compatibility work.** krabka is greenfield and undeployed. There are no production users, no persisted state to migrate, and no clients pinned to a build. This roadmap adds no migration code, no `V2` variants kept beside `V1`, no `#[serde(default)]` for old raft logs, no default-off feature flags, and no deprecated API surface. When a record shape or an interface changes, it changes, and a developer deletes the local data directory.

**Divergence from Confluent.** No item here adds an endpoint, a header, a query parameter or an error code to the client-facing listener that `cp-schema-registry` does not define. The admin listener is a separate surface that no Confluent client reaches, and the operational endpoints M7 adds there, `/metrics` and `/readyz`, are outside this constraint. Every rule that M3, M4 and M5 add carries a verdict or a body captured from the container first. Where cp's behavior cannot be reproduced, the suite records the exception instead of a hand-written expectation.

**Packaging.** The Helm chart, the apko image definition and the operator's `SchemaRegistry` CRD stay in [`robot-head/crabka`](https://github.com/robot-head/crabka). The CRD belongs to that repository's operator crate, and the operator is not moving. M7 states where they live and links to them. It does not copy them here.

**Release automation.** This repository publishes no crate. The `krabka-*` names on crates.io are published from `robot-head/crabka`, and consumers here pin by git revision.

**A second dependency set.** rules_rs reads the same `Cargo.toml` and `Cargo.lock` that Cargo reads. No item adds a Bazel-side dependency list to keep in sync.

**Style-only sweeps.** No item rewrites prose or code in a file that it does not otherwise edit. M7 corrects the specific documentation statements that contradict the code, and stops there.

**Performance work.** M5 adds pagination and M7 adds body and connection limits, because a missing bound is a correctness problem. Beyond those bounds, no cache layer, no benchmark suite and no throughput target is planned until the correctness gates hold.

