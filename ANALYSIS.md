# ANALYSIS: Spark Log Analysis on AWS Cluster
Name: **Anna Hyunjung Kim**

This report documents my approach and findings for:

**Problem 1 — Log Level Distribution**

**Problem 2 — Cluster Usage Analysis**

It also summarizes performance observations (local vs. cluster), optimization choices, Spark Web UI screenshots, and explanations of the visualizations in Problem 2.


# Problem 1 — Log Level Distribution

## Input & Parsing

- Read: I read raw log lines from application_*/*.log.
 - used professor code to download the test files. 

- Extract timestamp, log level, and message via regex given by professor on the README file. 

- Convert timestamp with to_timestamp

## Outputs

1. problem1_counts.csv

```
log_level,count
ERROR,11259
INFO,27389482
WARN,9595

```
2. problem1_sample.csv 
```
17/06/09 17:28:26 INFO executor.CoarseGrainedExecutorBackend: Got assigned task 136036,INFO
17/06/06 22:05:04 INFO storage.BlockManager: Found block rdd_2_7 locally,INFO
"17/06/09 21:28:41 INFO storage.MemoryStore: Block rdd_3719_1 stored as bytes in memory (estimated size 16.0 B, free 3.3 MB)",INFO
"17/06/09 17:30:17 INFO python.PythonRunner: Times: total = 40, boot = -30, init = 70, finish = 0",INFO
17/06/09 15:45:31 INFO executor.CoarseGrainedExecutorBackend: Got assigned task 2696,INFO
```

3. problem1_summary.txt

```
Total log lines processed: 33,236,604
Total lines with log levels: 27,410,336
Unique log levels found: 3

Log level distribution:
  ERROR:     11,259 (  0.04%)
  INFO : 27,389,482 ( 99.92%)
  WARN :      9,595 (  0.04%)
````

## Key Findings (Problem 1)


- Total log lines processed: 33,236,604

- Total lines with log levels: 27,410,336

- Unique log levels found: 3

| Log level |     Count | Share (%) |
|----------:|----------:|----------:|
| ERROR     |     11,259 |    0.041 |
| INFO      | 27,389,482 |   99.924 |
| WARN      |      9,595 |    0.035 |
| **Total** | **27,410,336** | **100.000** |



# Problem 2 — Cluster Usage Analysis

- From each log file name like application_1485248649253_0052,
        I took two parts:

        the first number is the cluster_id (1485248649253)

        the second number is the app_number (0052)

- For each application_id,
        I looked at all its log lines and found:

        the earliest time → start_time

        the latest time → end_time

- Then I grouped by each cluster_id
        and made a summary showing:

        how many applications were in the cluster

        when the first app started

        when the last app ended

## output

1. problem2_timeline.csv: Time-series data for each application
```
cluster_id,application_id,app_number,start_time,end_time
1440487435730,application_1440487435730_0039,0039,2015-09-01 18:14:40,2015-09-01 18:19:50
1448006111297,application_1448006111297_0137,0137,2016-04-07 10:45:21,2016-04-07 11:28:11
1448006111297,application_1448006111297_0138,0138,2016-04-07 11:39:58,2016-04-07 12:22:08
1460011102909,application_1460011102909_0176,0176,2016-07-26 11:54:20,2016-07-26 12:19:25
1472621869829,application_1472621869829_0081,0081,2016-09-09 07:43:47,2016-09-09 07:44:12
1472621869829,application_1472621869829_0082,0082,2016-09-09 07:45:49,2016-09-09 07:45:59

```
2. problem2_cluster_summary.csv: Aggregated cluster statistics
```
cluster_id,num_applications,cluster_first_app,cluster_last_app
1485248649253,181,2017-01-24 17:00:28,2017-07-27 21:45:00
1472621869829,8,2016-09-09 07:43:47,2016-09-09 10:07:06
1448006111297,2,2016-04-07 10:45:21,2016-04-07 12:22:08
1440487435730,1,2015-09-01 18:14:40,2015-09-01 18:19:50
1460011102909,1,2016-07-26 11:54:20,2016-07-26 12:19:25
1474351042505,1,2016-11-18 22:30:06,2016-11-19 00:59:04
```
3. problem2_stats.txt: Overall summary statistics
```
Total unique clusters: 6
Total applications: 194
Average applications per cluster: 32.33

Most heavily used clusters:
  Cluster 1485248649253: 181 applications
  Cluster 1472621869829: 8 applications
  Cluster 1448006111297: 2 applications
  Cluster 1440487435730: 1 applications
  Cluster 1460011102909: 1 applications
  Cluster 1474351042505: 1 applications

```
4. problem2_bar_chart.png: Bar chart visualization

5. problem2_density_plot.png: Faceted density plot visualization
