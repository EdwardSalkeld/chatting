# Nix packages and releases

Chatting owns its build definition and dependency hash. The release flow makes
tested source releases available for deployments to consume, and a separate
workflow proposes each new release to the Lab deployment.

## Package interface

The flake exports these packages for `x86_64-linux` and `aarch64-linux`:

- `chatting-handler`: the Go handler, with its Go dependency hash in
  `nix/handler.nix` and the full Go test suite run during the Nix build.
  `chatting-handler --version` prints the base version and source revision,
  such as `0.1.0+ace9313`; builds outside Nix print `dev`.
- `chatting-worker`: a wrapper with Python 3.14. The worker has no
  third-party runtime dependencies. Its build compiles the application and
  runs the installed entry point's `--help` command.
- `chatting` and `default`: both executables together.

Build and check on the current system:

```sh
nix build .#chatting
nix flake check --no-update-lock-file --print-build-logs
nix fmt -- --check flake.nix nix/handler.nix nix/worker.nix
```

CI currently builds on x86_64 Linux. The aarch64 outputs are available for native
builders but are not cross-built by this CI job.

BBMB and the executor CLI are separate runtime prerequisites. Host services,
configuration, secrets, data paths and rollout procedures belong to the
deployment, not these packages. See [operations](operations.md).

A consuming flake can pin a published tag and optionally share its nixpkgs:

```nix
inputs.chatting = {
  url = "github:EdwardSalkeld/chatting/v0.1.123"; # example release tag
  inputs.nixpkgs.follows = "nixpkgs";
};

# Within the consumer's outputs:
# chatting.packages.${system}.chatting
# chatting.packages.${system}.chatting-handler
# chatting.packages.${system}.chatting-worker
```

The recipe files can also be used with `pkgs.callPackage` directly. A consumer
overriding nixpkgs should build/check the package with its own pin before rollout.
GitHub's release source archives include the flake, lock and recipes; there are
no separate binary assets in this source-release flow.

## Dependency changes

When `go.mod` or `go.sum` changes, refresh `vendorHash` in `nix/handler.nix` in
the same application PR. Temporarily set it to `lib.fakeHash` (or the
standard zero SHA-256 placeholder), build `.#chatting-handler.goModules`, and
replace the placeholder with Nix's reported actual hash. Then build/check the
real package. Never merge a placeholder. An unchanged hash can reuse a cached
fixed-output result, so use a placeholder to force recalculation when needed.

The CI package build catches stale hashes or inconsistent vendoring before a
release is published. Downstream updates then move the source and matching
hash together, without deployment-side hash scraping.

## Release publication

After the `CI` workflow passes for a push to this repository's `main`, the
`Release` workflow publishes `v0.1.<CI run number>` with generated release notes.
Using the CI run number makes retries target the same tag and keeps version
ordering independent of completion timing. These combined application releases
are separate from the historical `handler-vN` tags. The package's base version
continues to come from `pyproject.toml`; the release tag identifies the exact
tested source revision.

Publication requires all CI jobs, including the Nix package build, to succeed.
Failed runs, pull-request runs and fork results cannot publish. If main has
advanced, a stale completion is skipped. Existing tags must point directly at
the tested SHA; tags are never moved. A retry leaves an existing release
unchanged, or completes publication if the tag exists but release creation
previously failed. API errors other than an explicit 404 stop publication.

The release workflow uses only this repository's `GITHUB_TOKEN` with
`contents: write` in the release job. It neither checks out nor writes another
repository.

## Proposing a release to Lab

`Open Lab Deploy PR` runs when the `Release` workflow finishes. It looks up this
repository's latest release, and if the Lab repository pins an older release tag
it moves that tag, relocks the input and opens or updates a "Deploy chatting
vX.Y.Z" pull request there. It needs the `LAB_REPO_TOKEN` secret, which is its
only cross-repository credential.

- The release is looked up here, never taken from the triggering event, and a
  triggered run only moves Lab forwards.
- It does nothing while Lab does not pin a release tag.
- Run it by hand with a release tag to propose that release instead, including
  an older one to roll back.

Because the deployment builds the packages from this flake at a released tag,
a stale dependency hash or broken package fails this repository's CI before a
release exists, not the deployment's CI afterwards.

The release API policy is covered by:

```sh
node --test .github/scripts/release.test.cjs
```
