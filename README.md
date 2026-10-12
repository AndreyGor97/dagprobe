# DagProbe

Debug and test [Apache Airflow](https://airflow.apache.org/) DAGs **without installing or running Airflow**.

DagProbe imports your DAG files against a lightweight shim of the Airflow SDK, then lets you
lint them, preview their Jinja rendering, run tasks against YAML fixtures, and audit behavior
differences between Airflow 2.10 and 3.3 — all in a plain Python environment, in seconds,
with no Airflow installation, no database and no scheduler.

```console
$ pip install dagprobe
```

## What it does

| Command | Purpose |
|---|---|
| `dagprobe list` | Import a DAGs folder, list DAG ids and import errors (with timing) |
| `dagprobe lint` | Import + static lint for CI: duplicate task ids, `datetime.now()` in DAG code, missing `catchup=`, deprecated kwargs; `--changed-only` lints just the files changed vs a base branch |
| `dagprobe render` | Jinja preview: rendered `template_fields` per task (before → after) |
| `dagprobe run` | Task Test: execute a DAG's tasks against fixtures — `PythonOperator` callables run for real, `trigger_rule` and `BranchPythonOperator` branching follow Airflow semantics, in-memory DB connections read/write across tasks |
| `dagprobe audit 2to3` | Run the same DAG under the 2.10 and 3.3 behavior profiles and report divergences (e.g. XCom value unwrapping, task `status` changes) |

`lint` and `run` are CI-first: `--format text|json|junit` and `--fail-on error|warning|never`,
stable exit codes (`0` clean, `1` findings at gate level, `2` usage/setup error), JUnit output
for PR test reports. A ready GitHub Actions workflow ships in
[`examples/ci/github-actions.yml`](examples/ci/github-actions.yml).

## Quickstart

```console
# what's in my DAGs folder, and does everything even import?
$ dagprobe list --dags-folder dags

# run one DAG's tasks against fixtures (fake conns, tables, files)
$ dagprobe run --dags-folder dags --dag-id branch_etl \
    --fixtures fixtures/branch.yaml
```

Fixtures are a small YAML file — no Airflow metadata database involved:

```yaml
conns:
  dwh:
    conn_type: postgres
    tables:
      events:
        - {event: signup, n: 3}
        - {event: purchase, n: 1}
```

More runnable examples: [`examples/dags/`](examples/dags/) with matching
[`examples/fixtures/`](examples/fixtures/), including a deliberately broken DAG
(`examples/broken/`) that demonstrates the CI gate.

## How it works

DagProbe ships a versioned shim of the Airflow import surface (`airflow.sdk`,
`airflow.providers.standard`, hooks, operators). Your DAG file is imported against the shim,
so imports resolve without an Airflow install — typically in well under a second per file,
versus tens of seconds to boot a real Airflow environment. Task execution reuses the same
Jinja engine Airflow uses, so template rendering is comparable 1:1.

Behavior profiles: `run` defaults to the Airflow 3.3 profile; `dagprobe audit 2to3` runs the
same DAG under both the 2.10 legacy and the 3.3 profile and reports concrete behavioral
divergences per task — the things that break silently during a 2.x → 3.x migration.

## Scope and limitations

- Airflow version profiles: **2.10 and 3.3** (arbitrary version pinning is on the roadmap).
- SQL runs on an in-memory SQLite via an emulated engine — real warehouse dialects are out of scope.
- No scheduler, no real network/connections: tasks run in-process against fixtures.
- Static lint covers a focused rule set, not a general Python linter — pair it with `ruff`.

## Development

```console
$ git clone https://github.com/AndreyGor97/dagprobe
$ cd dagprobe && uv sync
$ uv run pytest
```

Requires Python 3.11+. Apache-2.0 licensed; not affiliated with the Apache Software Foundation.
