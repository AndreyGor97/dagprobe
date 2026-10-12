# Contributing to DagProbe

Thanks for your interest! Short version below; open an issue first for anything bigger
than a bug fix.

## What goes where

DagProbe follows an open-core layout. Keep the boundary in mind when choosing what to work on:

| Change | Where it belongs |
|---|---|
| Bug fixes in the shim, analyzer, linter, task runner, audit | Core (`src/dagprobe/`) — always welcome |
| New lint rules with a clear false-positive story | Core — open an issue with a motivating DAG example first |
| New Airflow SDK surface in the shim (imports that currently fail) | Core — please include the DAG file that needs it and the Airflow version |
| New behavior-profile semantics (2.10 / 3.3 divergences) | Core — each change needs a test DAG showing the divergence |
| New fake operators/hooks/connections in fixtures | Core if generic; **separate `dagprobe-plugin-*` package** if tied to a specific provider or vendor |
| Integrations (CI systems, dashboards, formatters) | Separate `dagprobe-plugin-*` package, not core |

Rule of thumb: core stays vendor-neutral and offline; anything that grows the dependency
list or couples core to a specific service belongs in a plugin package.

## Development

```console
$ git clone https://github.com/AndreyGor97/dagprobe
$ cd dagprobe && uv sync
$ uv run pytest          # full suite, coverage reported
$ uv run ruff check . && uv run mypy
```

Every behavior change ships with a test: a fixture DAG plus the expected finding/status.
PRs must keep the suite green on the supported Python versions (3.11–3.14).

## Reporting issues

For import failures: the DAG file (minimal repro preferred), the Airflow version it was
written for, and the full `dagprobe list` error output. For audit findings that look wrong:
the DAG, both profile versions' expected behavior, and what you expected instead.

## License

By contributing you agree that your contributions are licensed under the Apache-2.0 license.
