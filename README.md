# State recovery review example

A small, standalone standard-library Python example for reviewing state recovery
after a **process crash or lost write acknowledgement**. Its only application
state is an integer counter driven by generated integer events.

This repository starts with a new history. It contains no production modules,
datasets, credentials, account integrations, network clients or external actions.

## Run

Python 3.12 or newer; no dependencies to install:

```sh
python -B -m unittest discover -v
python -B demo.py
```

Tests use disposable directories and generated values. Crash-injection tests
terminate only their own child processes. CI targets Windows and Linux.

## Protocol

1. Store an event journal and its replay checkpoint in one canonical bundle.
2. Keep authorized hashes in a **separately configured trusted directory**.
3. Persist `pending(old, new)` there before replacing the data bundle.
4. Flush/fsync a temporary file, then atomically replace the target in the same directory.
5. Acknowledge the new hash in the trusted file after publication.
6. On restart, accept only an already authorized old/new hash. Unknown data
   leaves the intent open and blocks the next dependent save.
7. Replay every event and check derived values; do not inject a saved result
   as a substitute for computation.

Locks are cooperative, nonblocking OS locks, always acquired trusted-file first,
then data-file. An absent trusted file never authorizes existing data implicitly.

## What this does not establish

- **Not power-loss safety.** File fsync is used; directory fsync is used on POSIX,
  but not on Windows. Hardware, OS, network filesystem and metadata durability
  need separate validation. A lost acknowledgement means inspect/recover, not rollback.
- The caller must protect trusted paths with appropriate permissions. This
  example does not install ACLs, create users or authenticate storage administrators.
- Anyone able to roll back **both** files can defeat this local trust boundary.
  There is no remote/monotonic anchor, signature, key service or TPM integration.
- No adversarial filesystem/path-race certification, external exactly-once
  effects, multi-host coordination, audit retention or production deployment claim.
- Checkpoints have a simple format version; runtime/package attestation and
  migrations are intentionally outside this minimal example.

## Review requested

Please use the review issue template. Particularly useful feedback:

1. Missing crash/acknowledgement windows or lock-order problems.
2. Minimal trustworthy monotonic anchoring against coordinated rollback.
3. What must be tested before claiming Windows power-loss durability.
4. Additional negative tests for corrupt metadata, stale writers and unsafe paths.

Use synthetic examples only; do not post production state or secrets.

## License

A reuse license has not been selected. This publication is currently for review;
please discuss licensing before incorporating it into another project.

[Краткое описание на русском](README_RU.md)
