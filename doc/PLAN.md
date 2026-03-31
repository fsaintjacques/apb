# apb — Implementation Plan

## Chunks

| #  | Name                              | Depends on | Crate(s)     | Status |
|----|-----------------------------------|------------|--------------|--------|
| 01 | Project scaffold + descriptor parsing | —      | apb-core     | Done   |
| 02a | Type system                       | 01       | apb-core     | Done   |
| 02b | Schema mapping                    | 02a      | apb-core     | Done   |
| 02c | Validation report                 | 02b      | apb-core     | Done   |
| 03 | Transcoder — scalar fields        | 02b      | apb-core     | Done   |
| 04 | Transcoder — nested types         | 03       | apb-core     | Done   |
| 05+06 | CLI (DuckDB + IPC input)       | 04       | apb-cli      |        |

## Dependency graph

```
01 ──▶ 02a ──▶ 02b ──▶ 02c ──┐
                  │            │
                  └──▶ 03 ──▶ 04
                               │
                               ▼
                             05+06 (CLI)
```

Note: `apb-source` crate eliminated. DuckDB replaces file/remote source
adapters. Arrow IPC stdin covers piping from Flight/BQ/other tools.

## Detailed plans

- [plan-01-scaffold.md](plans/plan-01-scaffold.md)
- [plan-02a-type-system.md](plans/plan-02a-type-system.md)
- [plan-02b-schema-mapping.md](plans/plan-02b-schema-mapping.md)
- [plan-02c-validation-report.md](plans/plan-02c-validation-report.md)
- [plan-03-transcoder-scalars.md](plans/plan-03-transcoder-scalars.md)
- [plan-04-transcoder-nested.md](plans/plan-04-transcoder-nested.md)
- [plan-05-06-cli.md](plans/plan-05-06-cli.md) (replaces plan-05 + plan-06)
