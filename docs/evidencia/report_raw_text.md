# pandas vs PySpark - Home_and_Kitchen ladder (1/10/50/100%)

Generated: 2026-11-18T03:32:44+00:00

## Environment

| Item | Value |
|---|---|
| dataset | McAuley-Lab/Amazon-Reviews-2023 |
| python | 3.13.17 |
| os | Windows-11-10.0.26300-SP0 |
| cpu_logical | 8 |
| ram_total_gb | 15.3 |

## Ladder levels

| Level | Rows | Actual fraction | Sample (MB) | Seconds |
|---|---|---|---|---|
| 50pct | 33,706,117 | 50.0017% | 5123.8 | 96.025 |
| 100pct | 67,409,944 | 100.0000% | 9961.3 | 110.415 |

Fact rows are a deterministic hash fraction of the published 0core interaction table. Every level joins against the same full product catalog (shared Parquet), so the analysis is constant and only the data grows.

## Level 50pct (33,706,117 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| run | pandas | 1 | - | - | - | - | - | killed at system RAM 97% (available 506 MB); process tree 12984.2 MB private (10384.0 MB resident) at kill |
| run | pandas | 2 | - | - | - | - | - | killed at system RAM 98% (available 390 MB); process tree 12965.5 MB private (10548.5 MB resident) at kill |
| run | pandas | 3 | - | - | - | - | - | killed at system RAM 98% (available 260 MB); process tree 13130.7 MB private (10371.5 MB resident) at kill |
| read | spark | 1 | 33,706,117 rows | 10.715 | 609.1 | 181.3 | - |  |
| clean | spark | 1 | 33,706,117 kept; 0 dropped | 8.514 | 2174.3 | 1565.9 | - |  |
| derive | spark | 1 | 33,706,117 rows; 6 derived columns | 5.908 | 2304.7 | 138.8 | - |  |
| join | spark | 1 | 33,706,117 rows; 0 without metadata | 41.411 | 4867.4 | 2570.8 | - |  |
| aggregate_by_store | spark | 1 | 325,928 stores; top Amazon Basics (244,370 reviews, 2,076 products) | 125.174 | 5013.0 | 145.5 | - |  |
| aggregate_by_time | spark | 1 | 1,975 groups; top 2021-03 (110,053 reviews) | 80.271 | 5063.5 | 176.3 | - |  |
| write_parquet | spark | 1 | 2 tables; 9 Parquet part files | 161.319 | 5281.2 | 217.6 | - |  |

## Level 100pct (67,409,944 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| run | pandas | 1 | - | - | - | - | - | killed at system RAM 99% (available 138 MB); process tree 13894.2 MB private (8344.4 MB resident) at kill |
| run | pandas | 2 | - | - | - | - | - | killed at system RAM 99% (available 114 MB); process tree 14579.8 MB private (8434.4 MB resident) at kill |
| run | pandas | 3 | - | - | - | - | - | killed at system RAM 98% (available 239 MB); process tree 14162.1 MB private (10846.3 MB resident) at kill |
| pandas_naive_read_json | pandas_naive | 1 | - | 20.079 | 8354.0 | 8261.8 | - | RuntimeError: killed at system RAM 98% (available 383 MB); process tree 30305.4 MB private (8261.5 MB resident) at kill |
| read | spark | 1 | 67,409,944 rows | 14.312 | 611.6 | 187.4 | - |  |
| clean | spark | 1 | 67,409,944 kept; 0 dropped | 11.873 | 2434.7 | 1825.1 | - |  |
| derive | spark | 1 | 67,409,944 rows; 6 derived columns | 9.487 | 2549.2 | 163.9 | - |  |
| join | spark | 1 | 67,409,944 rows; 0 without metadata | 85.148 | 5026.2 | 2517.3 | - |  |
| aggregate_by_store | spark | 1 | 375,506 stores; top Amazon Basics (487,718 reviews, 2,196 products) | 233.255 | 5837.1 | 810.8 | - |  |
| aggregate_by_time | spark | 1 | 1,985 groups; top 2021-03 (220,140 reviews) | 136.943 | 5896.9 | 80.1 | - |  |
| write_parquet | spark | 1 | 2 tables; 11 Parquet part files | 214.643 | 5684.6 | 447.4 | - |  |

## Summary (per engine and level)

| Level | Engine | Runs | Total time mean (s) | Total time std (s) | Max peak RSS (MB) | Status |
|---|---|---|---|---|---|---|
| 50pct | spark | 1 | 433.31 | 0.0 | 5281.2 | ok |
| 100pct | spark | 1 | 705.66 | 0.0 | 5896.9 | ok |

## Notes

- Each (engine, level, repetition) runs in a fresh process, so the peak RSS belongs to that run alone and not to allocator or JVM memory kept from previous ones.
- Times are cumulative wall time per action, because nothing is cached: operation N re-executes operations 1..N.
- Peak RSS is total resident memory of the Python process plus its JVM children, sampled every 50 ms; it is not JVM heap.
- Frame (MB) is the deep in-memory size of the pandas DataFrame that the operation carries; Spark has no equivalent column.
- Averages and standard deviations are rounded to 4 decimals; the two engines use different rounding modes, so an exact tie may differ in the last decimal.
- No optimization is applied here: no cache, no broadcast hint, no repartition and no tuned shuffle.
- A failure is recorded in the Error column; its dependents are skipped and the run does not abort. A run that exceeds the timeout is recorded as such.
