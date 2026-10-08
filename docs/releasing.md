# Releasing

A release of this repository is an annotated tag `vX.Y.Z` on `main`. The push
of the tag starts [`publish.yml`](../.github/workflows/publish.yml), which
publishes the library crates to crates.io. No other workflow publishes them.

The workspace releases as one unit. Every published crate inherits
`[workspace.package] version`, so one tag names one version of all of them.

## The published crates

| Crate | Depends on |
| :--- | :--- |
| `krabka-schema-serde` | `krabka-units` |

`krabka-schema-registry` sets `publish = false`. It is the service, not a
library. Operators get it as the `ghcr.io/krabka-io/krabka-schema-registry`
image, which the `delivery` job of `ci.yml` pushes on each push to `main`, and
as the Helm chart, which the `helm-index` job of
[krabka-io/krabka-io.github.io](https://github.com/krabka-io/krabka-io.github.io)
packages into https://krabka.io/charts. Neither is part of a crates.io
release. A new member crate is published unless its manifest sets
`publish = false`.

`krabka-schema-serde` takes `repository`, `license`, `authors`, `edition` and
`rust-version` from the workspace. Its `include` list ships the library source,
its README, and the `LICENSE` and `NOTICE` that Apache-2.0 requires with a
distribution. Those two are symlinks to the repository-root files, and cargo
packages their contents. Tests and fixtures stay out. Cargo prints an "ignoring test"
warning for each `[[test]]` that the package leaves out. The warnings are
expected.

Every `krabka-*` normal dependency of a published crate carries a `version`.
`cargo publish` ignores `[patch.crates-io]` and keeps only the `version` of a
`path` or git dependency, so the published crate resolves each of them from
crates.io. The in-process `krabka-broker` that the test suites start is not on
crates.io. It is a git dev-dependency without a `version`, so cargo drops it
from the published manifest.

The other krabka repositories publish their crates from their own workflows.
`krabka-schema-serde` depends on `krabka-units`, so crates.io must have a
release of krabka-protocol first. `krabka-broker` depends on
`krabka-schema-serde`, so a release of this repository comes before one of
krabka-broker. The order is:

1. krabka-protocol
2. krabka-client-rs
3. krabka-schema-registry
4. krabka-broker

## 1. Prepare the version

Set the new version in these places:

- `[workspace.package] version` in the root `Cargo.toml`.
- The `krabka-schema-serde` requirement in `[workspace.dependencies]`. A path
  dependency also carries a version, and `cargo publish` uses that version in
  the published manifest.
- `version` in `MODULE.bazel`.
- `appVersion` in `charts/krabka-schema-registry/Chart.yaml`. The `image` job
  of `ci.yml` fails when it differs from the workspace version.

Then run `cargo update --workspace` to update `Cargo.lock`, and
`bazel mod deps --lockfile_mode=update` to update `MODULE.bazel.lock`.

Before you merge, run a dry run from the branch: start `publish.yml` from the
Actions tab with `dry_run` on. It packages each crate and builds it from the
packaged sources, as crates.io users get them. Locally, the same check is:

```sh
cargo publish -p krabka-schema-serde --dry-run
```

## 2. Tag the release

Tag the merge commit on `main`, then push the tag:

```sh
git tag -a v0.4.2 -m "krabka-schema-registry 0.4.2"
git push origin v0.4.2
```

## 3. What the workflow does

The `plan` job holds no credential. It:

1. stops unless the tagged commit is an ancestor of `origin/main`.
2. stops unless a `push` run of `ci.yml` passed on that commit.
3. stops unless every published crate has the version that the tag names.
4. asks the crates.io API which crate versions exist, and keeps the others.
5. runs `cargo publish --dry-run` over the crates that it kept. Cargo resolves
   a pending sibling from the packages it has just made, and every other
   dependency from crates.io.

The `publish` job runs in the `crates-io` environment. It uploads the pending
crates one at a time, in dependency order. Cargo waits until each crate is in
the index before it uploads the next one.

A rerun is safe. The `plan` job skips each version that crates.io already
has, so a rerun after a failure uploads only the rest.

A manual run takes two inputs:

- `dry_run`, on by default. Off, the run uploads, and it must start from a
  `v*` tag.
- `crates`, a space-separated list of crate names. A run with a list publishes
  only those crates. Use it when one crate cannot publish and the others must
  not wait for it.

## Credentials: bootstrap, then trusted publishing

The `publish` job authenticates with one of two credentials:

- **A token.** When the `CARGO_REGISTRY_TOKEN` secret of the `crates-io`
  environment is set, the job uses it.
- **Trusted publishing.** When that secret is not set, the job runs
  [`rust-lang/crates-io-auth-action`](https://github.com/rust-lang/crates-io-auth-action).
  The action exchanges the job's GitHub OIDC token for a crates.io token. That
  token expires after 30 minutes, and the action revokes it when the job ends.
  No long-lived secret exists.

crates.io allows trusted publishing only for a crate that already exists. So
the first release of each crate name needs the token, and every later release
uses trusted publishing.

### Once: the GitHub environment

In the repository settings, open **Environments** and create `crates-io`. Under
**Deployment branches and tags**, allow only tags that match `v*`. Add required
reviewers if a person should approve each publish.

### First publish of a crate name

1. Sign in to crates.io with the account that will own the crates. Under
   **Account Settings**, open **API Tokens** and create a token with the
   `publish-new` and `publish-update` scopes. Limit it to the crate pattern
   `krabka-*` and give it a short expiry.
2. Add the token to the `crates-io` environment as the secret
   `CARGO_REGISTRY_TOKEN`.
3. Push the release tag, or rerun `publish.yml` on it.

crates.io limits new crate names to a burst of five, then one every ten
minutes. On a `429` answer the job waits ten minutes and tries again.

### Then: trusted publishing for each crate

For each published crate:

1. On crates.io, open the crate, then **Settings**, then **Trusted
   Publishing**.
2. Add a GitHub publisher with these values:
   - Repository owner: `krabka-io`
   - Repository name: `krabka-schema-registry`
   - Workflow filename: `publish.yml`
   - Environment: `crates-io`

When every published crate has a publisher, delete the `CARGO_REGISTRY_TOKEN`
secret, and revoke the token on crates.io. The next release uses trusted
publishing. Its log says "publishing through trusted publishing".

A crate that joins the published set later needs the token once, for its
first release. Add the secret again for that release, configure the new
crate's publisher, and delete the secret again.

## The crabka-schema-serde crate

Krabka was called Crabka, and robot-head published `crabka-schema-serde` from
`robot-head/crabka`. krabka-protocol's
[`retire-crabka.yml`](https://github.com/krabka-io/krabka-protocol/blob/main/.github/workflows/retire-crabka.yml)
retires it, with every other `crabka-*` crate, once each `krabka-*` successor
is on crates.io. Its tombstone release links to `krabka-schema-serde`, so the
first publish from this repository must come before that run.
