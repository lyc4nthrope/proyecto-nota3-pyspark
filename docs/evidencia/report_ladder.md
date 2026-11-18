# pandas vs PySpark - Home_and_Kitchen ladder (1/10/50/100%)

Generated: 2026-11-18T00:32:50+00:00

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
| 1pct | 665,617 | 0.9991% | 28.4 | 122.324 |
| 10pct | 6,664,148 | 10.0026% | 243.0 | 129.552 |
| 50pct | 33,313,034 | 50.0016% | 970.5 | 117.559 |
| 100pct | 66,623,880 | 100.0000% | 1715.3 | 88.259 |

Fact rows are a deterministic hash fraction of the published 0core interaction table. Every level joins against the same full product catalog (shared Parquet), so the analysis is constant and only the data grows.

## Level 1pct (665,617 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| read | pandas | 1 | 665,617 rows | 0.381 | 200.0 | 107.7 | 44.4 |  |
| clean | pandas | 1 | 665,617 kept; 0 dropped | 0.026 | 201.6 | 1.6 | 44.4 |  |
| derive | pandas | 1 | 665,617 rows; 6 derived columns | 0.661 | 252.8 | 51.2 | 85.1 |  |
| join | pandas | 1 | 665,617 rows; 0 without metadata | 3.383 | 1107.0 | 854.2 | 124.7 |  |
| aggregate_by_store | pandas | 1 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 0.668 | 912.7 | 0.0 | 4.0 |  |
| aggregate_by_time | pandas | 1 | 1,704 groups; top 2021-03 (2,186 reviews) | 0.130 | 419.4 | 37.7 | 0.1 |  |
| write_parquet | pandas | 1 | 2 tables; 2 Parquet part files | 0.103 | 388.5 | 5.4 | - |  |
| read | pandas | 2 | 665,617 rows | 0.185 | 200.1 | 108.2 | 44.4 |  |
| clean | pandas | 2 | 665,617 kept; 0 dropped | 0.016 | 200.3 | 0.2 | 44.4 |  |
| derive | pandas | 2 | 665,617 rows; 6 derived columns | 0.612 | 252.8 | 52.5 | 85.1 |  |
| join | pandas | 2 | 665,617 rows; 0 without metadata | 2.937 | 1149.6 | 896.9 | 124.7 |  |
| aggregate_by_store | pandas | 2 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 0.700 | 752.2 | 2.0 | 4.0 |  |
| aggregate_by_time | pandas | 2 | 1,704 groups; top 2021-03 (2,186 reviews) | 0.155 | 398.6 | 25.6 | 0.1 |  |
| write_parquet | pandas | 2 | 2 tables; 2 Parquet part files | 0.077 | 378.0 | 3.5 | - |  |
| read | pandas | 3 | 665,617 rows | 0.205 | 199.6 | 107.5 | 44.4 |  |
| clean | pandas | 3 | 665,617 kept; 0 dropped | 0.015 | 201.3 | 1.6 | 44.4 |  |
| derive | pandas | 3 | 665,617 rows; 6 derived columns | 0.660 | 252.4 | 51.1 | 85.1 |  |
| join | pandas | 3 | 665,617 rows; 0 without metadata | 2.986 | 1153.5 | 901.1 | 124.7 |  |
| aggregate_by_store | pandas | 3 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 0.570 | 796.9 | 0.0 | 4.0 |  |
| aggregate_by_time | pandas | 3 | 1,704 groups; top 2021-03 (2,186 reviews) | 0.102 | 363.1 | 39.6 | 0.1 |  |
| write_parquet | pandas | 3 | 2 tables; 2 Parquet part files | 0.081 | 335.1 | 10.1 | - |  |
| read | spark | 1 | 665,617 rows | 9.088 | 551.7 | 127.9 | - |  |
| clean | spark | 1 | 665,617 kept; 0 dropped | 2.232 | 794.4 | 244.5 | - |  |
| derive | spark | 1 | 665,617 rows; 6 derived columns | 1.822 | 899.9 | 103.4 | - |  |
| join | spark | 1 | 665,617 rows; 0 without metadata | 9.120 | 1935.4 | 1039.5 | - |  |
| aggregate_by_store | spark | 1 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 20.590 | 2760.9 | 825.5 | - |  |
| aggregate_by_time | spark | 1 | 1,704 groups; top 2021-03 (2,186 reviews) | 10.494 | 2817.1 | 59.0 | - |  |
| write_parquet | spark | 1 | 2 tables; 5 Parquet part files | 17.253 | 3051.6 | 245.4 | - |  |
| read | spark | 2 | 665,617 rows | 8.639 | 580.2 | 113.4 | - |  |
| clean | spark | 2 | 665,617 kept; 0 dropped | 2.231 | 805.2 | 228.6 | - |  |
| derive | spark | 2 | 665,617 rows; 6 derived columns | 1.779 | 834.9 | 29.7 | - |  |
| join | spark | 2 | 665,617 rows; 0 without metadata | 9.079 | 1619.9 | 784.6 | - |  |
| aggregate_by_store | spark | 2 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 20.052 | 2834.8 | 1252.9 | - |  |
| aggregate_by_time | spark | 2 | 1,704 groups; top 2021-03 (2,186 reviews) | 11.289 | 3091.2 | 258.8 | - |  |
| write_parquet | spark | 2 | 2 tables; 5 Parquet part files | 17.364 | 3096.6 | 8.0 | - |  |
| read | spark | 3 | 665,617 rows | 9.426 | 546.1 | 122.4 | - |  |
| clean | spark | 3 | 665,617 kept; 0 dropped | 2.250 | 768.3 | 222.1 | - |  |
| derive | spark | 3 | 665,617 rows; 6 derived columns | 1.791 | 813.9 | 45.6 | - |  |
| join | spark | 3 | 665,617 rows; 0 without metadata | 8.233 | 1769.5 | 954.6 | - |  |
| aggregate_by_store | spark | 3 | 84,436 stores; top Amazon Basics (4,860 reviews, 625 products) | 18.940 | 3143.2 | 1373.7 | - |  |
| aggregate_by_time | spark | 3 | 1,704 groups; top 2021-03 (2,186 reviews) | 17.647 | 3189.5 | 46.5 | - |  |
| write_parquet | spark | 3 | 2 tables; 5 Parquet part files | 19.360 | 2740.1 | 678.5 | - |  |

## Level 10pct (6,664,148 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| read | pandas | 1 | 6,664,148 rows | 1.033 | 838.1 | 745.9 | 444.9 |  |
| clean | pandas | 1 | 6,664,148 kept; 0 dropped | 0.123 | 882.9 | 44.9 | 444.9 |  |
| derive | pandas | 1 | 6,664,148 rows; 6 derived columns | 5.752 | 1354.3 | 515.9 | 851.6 |  |
| join | pandas | 1 | 6,664,148 rows; 0 without metadata | 13.495 | 2840.0 | 1492.1 | 1248.8 |  |
| aggregate_by_store | pandas | 1 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 5.463 | 2012.1 | 146.9 | 10.1 |  |
| aggregate_by_time | pandas | 1 | 1,947 groups; top 2021-03 (21,885 reviews) | 0.735 | 1851.6 | 264.4 | 0.1 |  |
| write_parquet | pandas | 1 | 2 tables; 2 Parquet part files | 0.155 | 1599.7 | 12.5 | - |  |
| read | pandas | 2 | 6,664,148 rows | 0.750 | 857.4 | 765.2 | 444.9 |  |
| clean | pandas | 2 | 6,664,148 kept; 0 dropped | 0.117 | 864.1 | 6.7 | 444.9 |  |
| derive | pandas | 2 | 6,664,148 rows; 6 derived columns | 6.047 | 1392.7 | 534.9 | 851.6 |  |
| join | pandas | 2 | 6,664,148 rows; 0 without metadata | 12.438 | 2804.3 | 1437.1 | 1248.8 |  |
| aggregate_by_store | pandas | 2 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 5.117 | 1998.5 | 129.2 | 10.1 |  |
| aggregate_by_time | pandas | 2 | 1,947 groups; top 2021-03 (21,885 reviews) | 0.707 | 1861.5 | 265.2 | 0.1 |  |
| write_parquet | pandas | 2 | 2 tables; 2 Parquet part files | 0.144 | 1605.6 | 9.2 | - |  |
| read | pandas | 3 | 6,664,148 rows | 0.711 | 856.5 | 764.2 | 444.9 |  |
| clean | pandas | 3 | 6,664,148 kept; 0 dropped | 0.147 | 901.4 | 44.8 | 444.9 |  |
| derive | pandas | 3 | 6,664,148 rows; 6 derived columns | 5.704 | 1372.7 | 515.9 | 851.6 |  |
| join | pandas | 3 | 6,664,148 rows; 0 without metadata | 12.063 | 2801.7 | 1435.3 | 1248.8 |  |
| aggregate_by_store | pandas | 3 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 5.403 | 2006.9 | 155.9 | 10.1 |  |
| aggregate_by_time | pandas | 3 | 1,947 groups; top 2021-03 (21,885 reviews) | 0.733 | 1833.0 | 255.9 | 0.1 |  |
| write_parquet | pandas | 3 | 2 tables; 2 Parquet part files | 0.150 | 1586.4 | 9.1 | - |  |
| read | spark | 1 | 6,664,148 rows | 9.602 | 558.1 | 139.7 | - |  |
| clean | spark | 1 | 6,664,148 kept; 0 dropped | 3.728 | 1759.8 | 1201.6 | - |  |
| derive | spark | 1 | 6,664,148 rows; 6 derived columns | 3.043 | 2383.3 | 622.5 | - |  |
| join | spark | 1 | 6,664,148 rows; 0 without metadata | 16.114 | 3248.2 | 890.8 | - |  |
| aggregate_by_store | spark | 1 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 37.410 | 4013.6 | 788.7 | - |  |
| aggregate_by_time | spark | 1 | 1,947 groups; top 2021-03 (21,885 reviews) | 20.341 | 4120.2 | 108.2 | - |  |
| write_parquet | spark | 1 | 2 tables; 9 Parquet part files | 32.635 | 4392.5 | 1078.3 | - |  |
| read | spark | 2 | 6,664,148 rows | 9.634 | 562.9 | 151.5 | - |  |
| clean | spark | 2 | 6,664,148 kept; 0 dropped | 4.309 | 1727.9 | 1165.8 | - |  |
| derive | spark | 2 | 6,664,148 rows; 6 derived columns | 3.310 | 1873.3 | 145.4 | - |  |
| join | spark | 2 | 6,664,148 rows; 0 without metadata | 14.496 | 3102.9 | 1228.5 | - |  |
| aggregate_by_store | spark | 2 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 35.129 | 4271.4 | 1180.5 | - |  |
| aggregate_by_time | spark | 2 | 1,947 groups; top 2021-03 (21,885 reviews) | 19.976 | 4358.1 | 93.6 | - |  |
| write_parquet | spark | 2 | 2 tables; 9 Parquet part files | 32.588 | 5019.5 | 661.5 | - |  |
| read | spark | 3 | 6,664,148 rows | 11.139 | 551.4 | 126.8 | - |  |
| clean | spark | 3 | 6,664,148 kept; 0 dropped | 5.033 | 1457.2 | 907.9 | - |  |
| derive | spark | 3 | 6,664,148 rows; 6 derived columns | 2.703 | 1772.7 | 317.2 | - |  |
| join | spark | 3 | 6,664,148 rows; 0 without metadata | 14.180 | 2712.5 | 939.8 | - |  |
| aggregate_by_store | spark | 3 | 213,606 stores; top Amazon Basics (48,242 reviews, 1,536 products) | 36.792 | 4163.7 | 1451.1 | - |  |
| aggregate_by_time | spark | 3 | 1,947 groups; top 2021-03 (21,885 reviews) | 20.279 | 4641.7 | 478.0 | - |  |
| write_parquet | spark | 3 | 2 tables; 9 Parquet part files | 31.548 | 4837.6 | 1149.6 | - |  |

## Level 50pct (33,313,034 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| read | pandas | 1 | 33,313,034 rows | 3.384 | 3422.6 | 3330.4 | 2223.9 |  |
| clean | pandas | 1 | 33,313,034 kept; 0 dropped | 0.709 | 3645.3 | 222.7 | 2223.9 |  |
| derive | pandas | 1 | 33,313,034 rows; 6 derived columns | 33.352 | 6070.0 | 2647.0 | 4257.2 |  |
| join | pandas | 1 | 33,313,034 rows; 0 without metadata | 78.432 | 8107.3 | 2145.7 | 6242.9 |  |
| aggregate_by_store | pandas | 1 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 36.136 | 6624.6 | 1118.1 | 15.5 |  |
| aggregate_by_time | pandas | 1 | 1,975 groups; top 2021-03 (108,769 reviews) | 6.406 | 6650.5 | 1271.3 | 0.1 |  |
| write_parquet | pandas | 1 | 2 tables; 2 Parquet part files | 0.567 | 5243.9 | 12.7 | - |  |
| read | pandas | 2 | 33,313,034 rows | 4.339 | 3410.4 | 3318.1 | 2223.9 |  |
| clean | pandas | 2 | 33,313,034 kept; 0 dropped | 0.834 | 3633.2 | 222.8 | 2223.9 |  |
| derive | pandas | 2 | 33,313,034 rows; 6 derived columns | 36.682 | 5994.3 | 2583.5 | 4257.2 |  |
| join | pandas | 2 | 33,313,034 rows; 0 without metadata | 58.338 | 7514.1 | 1557.2 | 6242.9 |  |
| aggregate_by_store | pandas | 2 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 34.069 | 5866.9 | 1085.2 | 15.5 |  |
| aggregate_by_time | pandas | 2 | 1,975 groups; top 2021-03 (108,769 reviews) | 5.124 | 6256.4 | 1499.0 | 0.1 |  |
| write_parquet | pandas | 2 | 2 tables; 2 Parquet part files | 0.250 | 4986.4 | 9.3 | - |  |
| read | pandas | 3 | 33,313,034 rows | 3.514 | 3438.2 | 3345.9 | 2223.9 |  |
| clean | pandas | 3 | 33,313,034 kept; 0 dropped | 0.564 | 3660.9 | 222.7 | 2223.9 |  |
| derive | pandas | 3 | 33,313,034 rows; 6 derived columns | 32.525 | 6106.7 | 2668.2 | 4257.2 |  |
| join | pandas | 3 | 33,313,034 rows; 0 without metadata | 59.753 | 6686.1 | 701.5 | 6242.9 |  |
| aggregate_by_store | pandas | 3 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 29.658 | 5506.5 | 1029.6 | 15.5 |  |
| aggregate_by_time | pandas | 3 | 1,975 groups; top 2021-03 (108,769 reviews) | 4.478 | 5899.2 | 1498.8 | 0.1 |  |
| write_parquet | pandas | 3 | 2 tables; 2 Parquet part files | 0.260 | 4633.8 | 11.6 | - |  |
| read | spark | 1 | 33,313,034 rows | 8.841 | 552.4 | 127.9 | - |  |
| clean | spark | 1 | 33,313,034 kept; 0 dropped | 6.607 | 2657.5 | 2105.1 | - |  |
| derive | spark | 1 | 33,313,034 rows; 6 derived columns | 5.081 | 3260.8 | 631.2 | - |  |
| join | spark | 1 | 33,313,034 rows; 0 without metadata | 31.311 | 5608.8 | 2441.9 | - |  |
| aggregate_by_store | spark | 1 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 80.754 | 6094.7 | 507.5 | - |  |
| aggregate_by_time | spark | 1 | 1,975 groups; top 2021-03 (108,769 reviews) | 57.903 | 6277.0 | 285.9 | - |  |
| write_parquet | spark | 1 | 2 tables; 9 Parquet part files | 73.030 | 6094.2 | 170.4 | - |  |
| read | spark | 2 | 33,313,034 rows | 9.262 | 555.4 | 133.2 | - |  |
| clean | spark | 2 | 33,313,034 kept; 0 dropped | 6.267 | 3365.8 | 2811.1 | - |  |
| derive | spark | 2 | 33,313,034 rows; 6 derived columns | 4.003 | 3836.7 | 470.8 | - |  |
| join | spark | 2 | 33,313,034 rows; 0 without metadata | 22.187 | 5921.6 | 2084.7 | - |  |
| aggregate_by_store | spark | 2 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 53.831 | 6442.1 | 529.6 | - |  |
| aggregate_by_time | spark | 2 | 1,975 groups; top 2021-03 (108,769 reviews) | 39.779 | 6557.5 | 198.0 | - |  |
| write_parquet | spark | 2 | 2 tables; 9 Parquet part files | 65.136 | 5928.6 | 117.3 | - |  |
| read | spark | 3 | 33,313,034 rows | 11.071 | 617.1 | 193.2 | - |  |
| clean | spark | 3 | 33,313,034 kept; 0 dropped | 10.109 | 2912.3 | 2295.2 | - |  |
| derive | spark | 3 | 33,313,034 rows; 6 derived columns | 7.187 | 3636.6 | 1769.8 | - |  |
| join | spark | 3 | 33,313,034 rows; 0 without metadata | 39.638 | 5515.0 | 2402.3 | - |  |
| aggregate_by_store | spark | 3 | 325,924 stores; top Amazon Basics (241,763 reviews, 2,076 products) | 82.678 | 6043.0 | 529.0 | - |  |
| aggregate_by_time | spark | 3 | 1,975 groups; top 2021-03 (108,769 reviews) | 47.221 | 6259.8 | 301.7 | - |  |
| write_parquet | spark | 3 | 2 tables; 9 Parquet part files | 61.348 | 5263.8 | 169.0 | - |  |

## Level 100pct (66,623,880 rows)

| Operation | Engine | Rep | Result | Time (s) | Peak RSS (MB) | Delta (MB) | Frame (MB) | Error |
|---|---|---|---|---|---|---|---|---|
| read | pandas | 1 | 66,623,880 rows | 4.072 | 6287.7 | 6195.4 | 4447.7 |  |
| clean | pandas | 1 | 66,623,880 kept; 0 dropped | 0.904 | 6732.8 | 445.1 | 4447.7 |  |
| derive | pandas | 1 | 66,623,880 rows; 6 derived columns | 35.502 | 8091.6 | 1803.5 | 8514.1 |  |
| join | pandas | 1 | 66,623,880 rows; 0 without metadata | 95.892 | 10183.9 | 3156.5 | 12485.4 |  |
| aggregate_by_store | pandas | 1 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 43.863 | 9556.0 | 1593.2 | 18.0 |  |
| aggregate_by_time | pandas | 1 | 1,985 groups; top 2021-03 (217,621 reviews) | 10.285 | 6121.2 | 3320.1 | 0.1 |  |
| write_parquet | pandas | 1 | 2 tables; 2 Parquet part files | 0.306 | 3577.2 | 12.1 | - |  |
| read | pandas | 2 | 66,623,880 rows | 4.504 | 6292.8 | 6200.5 | 4447.7 |  |
| clean | pandas | 2 | 66,623,880 kept; 0 dropped | 1.053 | 6801.5 | 508.6 | 4447.7 |  |
| derive | pandas | 2 | 66,623,880 rows; 6 derived columns | 52.190 | 9721.5 | 3428.3 | 8514.1 |  |
| join | pandas | 2 | 66,623,880 rows; 0 without metadata | 117.780 | 10101.9 | 2181.9 | 12485.4 |  |
| aggregate_by_store | pandas | 2 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 50.713 | 10243.8 | 2281.2 | 18.0 |  |
| aggregate_by_time | pandas | 2 | 1,985 groups; top 2021-03 (217,621 reviews) | 10.076 | 9807.4 | 2414.5 | 0.1 |  |
| write_parquet | pandas | 2 | 2 tables; 2 Parquet part files | 0.222 | 2647.8 | 12.4 | - |  |
| read | pandas | 3 | 66,623,880 rows | 4.035 | 6344.7 | 6252.3 | 4447.7 |  |
| clean | pandas | 3 | 66,623,880 kept; 0 dropped | 1.139 | 6789.8 | 445.1 | 4447.7 |  |
| derive | pandas | 3 | 66,623,880 rows; 6 derived columns | 31.164 | 9335.4 | 2990.3 | 8514.1 |  |
| join | pandas | 3 | 66,623,880 rows; 0 without metadata | 100.744 | 10087.0 | 2263.4 | 12485.4 |  |
| aggregate_by_store | pandas | 3 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 44.355 | 9702.9 | 1739.6 | 18.0 |  |
| aggregate_by_time | pandas | 3 | 1,985 groups; top 2021-03 (217,621 reviews) | 6.451 | 6338.2 | 3304.4 | 0.1 |  |
| write_parquet | pandas | 3 | 2 tables; 2 Parquet part files | 0.199 | 3809.8 | 12.6 | - |  |
| read | spark | 1 | 66,623,880 rows | 5.862 | 588.1 | 116.4 | - |  |
| clean | spark | 1 | 66,623,880 kept; 0 dropped | 6.728 | 4081.8 | 3500.9 | - |  |
| derive | spark | 1 | 66,623,880 rows; 6 derived columns | 5.129 | 4902.3 | 942.8 | - |  |
| join | spark | 1 | 66,623,880 rows; 0 without metadata | 33.425 | 6147.3 | 2562.4 | - |  |
| aggregate_by_store | spark | 1 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 77.443 | 7071.3 | 945.4 | - |  |
| aggregate_by_time | spark | 1 | 1,985 groups; top 2021-03 (217,621 reviews) | 48.177 | 7432.7 | 582.5 | - |  |
| write_parquet | spark | 1 | 2 tables; 9 Parquet part files | 80.528 | 7147.4 | 295.1 | - |  |
| read | spark | 2 | 66,623,880 rows | 5.149 | 603.7 | 175.5 | - |  |
| clean | spark | 2 | 66,623,880 kept; 0 dropped | 6.232 | 4159.8 | 3555.0 | - |  |
| derive | spark | 2 | 66,623,880 rows; 6 derived columns | 4.996 | 5716.1 | 1654.4 | - |  |
| join | spark | 2 | 66,623,880 rows; 0 without metadata | 33.791 | 7604.3 | 2038.2 | - |  |
| aggregate_by_store | spark | 2 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 107.658 | 8165.0 | 573.5 | - |  |
| aggregate_by_time | spark | 2 | 1,985 groups; top 2021-03 (217,621 reviews) | 70.080 | 7995.3 | 577.3 | - |  |
| write_parquet | spark | 2 | 2 tables; 9 Parquet part files | 114.631 | 7451.0 | 519.3 | - |  |
| read | spark | 3 | 66,623,880 rows | 5.490 | 569.1 | 146.1 | - |  |
| clean | spark | 3 | 66,623,880 kept; 0 dropped | 6.761 | 3727.9 | 3158.4 | - |  |
| derive | spark | 3 | 66,623,880 rows; 6 derived columns | 5.715 | 5326.4 | 1797.3 | - |  |
| join | spark | 3 | 66,623,880 rows; 0 without metadata | 38.884 | 6371.2 | 1166.2 | - |  |
| aggregate_by_store | spark | 3 | 375,506 stores; top Amazon Basics (482,476 reviews, 2,196 products) | 92.671 | 6965.0 | 626.6 | - |  |
| aggregate_by_time | spark | 3 | 1,985 groups; top 2021-03 (217,621 reviews) | 61.423 | 7426.2 | 575.5 | - |  |
| write_parquet | spark | 3 | 2 tables; 9 Parquet part files | 122.404 | 6249.9 | 132.9 | - |  |

## Summary (per engine and level)

| Level | Engine | Runs | Total time mean (s) | Total time std (s) | Max peak RSS (MB) | Status |
|---|---|---|---|---|---|---|
| 1pct | pandas | 3 | 4.88 | 0.41 | 1153.5 | ok |
| 1pct | spark | 3 | 72.89 | 4.12 | 3189.5 | ok |
| 10pct | pandas | 3 | 25.66 | 0.97 | 2840.0 | ok |
| 10pct | spark | 3 | 121.33 | 1.74 | 5019.5 | ok |
| 50pct | pandas | 3 | 143.12 | 14.44 | 8107.3 | ok |
| 50pct | spark | 3 | 241.08 | 35.24 | 6557.5 | ok |
| 100pct | pandas | 3 | 205.15 | 27.22 | 10243.8 | ok |
| 100pct | spark | 3 | 311.06 | 46.79 | 8165.0 | ok |

## Notes

- Each (engine, level, repetition) runs in a fresh process, so the peak RSS belongs to that run alone and not to allocator or JVM memory kept from previous ones.
- Times are cumulative wall time per action, because nothing is cached: operation N re-executes operations 1..N.
- Peak RSS is total resident memory of the Python process plus its JVM children, sampled every 50 ms; it is not JVM heap.
- Frame (MB) is the deep in-memory size of the pandas DataFrame that the operation carries; Spark has no equivalent column.
- Averages and standard deviations are rounded to 4 decimals; the two engines use different rounding modes, so an exact tie may differ in the last decimal.
- No optimization is applied here: no cache, no broadcast hint, no repartition and no tuned shuffle.
- A failure is recorded in the Error column; its dependents are skipped and the run does not abort. A run that exceeds the timeout is recorded as such.
