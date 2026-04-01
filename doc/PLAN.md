# apb — Implementation Plan

## Chunks

| #     | Name                                     | Status |
|-------|------------------------------------------|--------|
| 01    | Project scaffold + descriptor parsing    | Done   |
| 02a   | Type system                              | Done   |
| 02b   | Schema mapping                           | Done   |
| 02c   | Validation report                        | Done   |
| 03    | Transcoder — scalar fields               | Done   |
| 04    | Transcoder — nested types                | Done   |
| 05+06 | CLI (DuckDB + IPC input, 3 output modes) | Done   |
|       | Well-known type encoders (Timestamp/Duration) | Done |
|       | String → enum encoding + `--coerce`      | Done   |
|       | `--unknown-enum` flag                    | Done   |
|       | Structured logging (`-v`/`-vv`)          | Done   |
|       | Validate output redesign (colored, IDL)  | Done   |
|       | Partial struct matching for repeated/map | Done   |

## Detailed plans

- [plan-01-scaffold.md](plans/plan-01-scaffold.md)
- [plan-02a-type-system.md](plans/plan-02a-type-system.md)
- [plan-02b-schema-mapping.md](plans/plan-02b-schema-mapping.md)
- [plan-02c-validation-report.md](plans/plan-02c-validation-report.md)
- [plan-03-transcoder-scalars.md](plans/plan-03-transcoder-scalars.md)
- [plan-04-transcoder-nested.md](plans/plan-04-transcoder-nested.md)
- [plan-05-06-cli.md](plans/plan-05-06-cli.md) (replaces plan-05 + plan-06)

## Future work

- `apb generate` — Arrow schema → proto descriptor/IDL
- C ABI (`apb-cabi`) for non-Rust consumers
- Proto → Arrow decode (reverse transcoding)
- Performance (SIMD, vectorized encoding)
