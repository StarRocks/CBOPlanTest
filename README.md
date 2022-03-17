# CBOPlannerTest
This project used to test the correctness and performance the new Planner of starrocks

## 1 Compile
```dtd
mvn package
```
## 2 Modify config（config.properties）
HTTP_PORT is the http port in fe.conf

```dtd
CONNECT_URL = jdbc:mysql://127.0.0.1:8880/test
USER = root
PASS = 123
HTTP_PORT = 1234
```

Other optional configurations:
1. Whether the slow query collects Profile and Explain, default false
2. FRAGMENT_INSTANCE: parallel instance of starrocks, default follow starrocks session variable 
3. COLLECT_RESULT_DATA: whether to collect the result set of the query when the result is wrong, default false
4. COLLECT_STATISTIC: whether to collect statistics (column max, min, number of null), default false
```dtd
SLOW_PROFILE = true
SLOW_EXPLAIN = true
FRAGMENT_INSTANCE = 16
COLLECT_RESULT_DATA = true
COLLECT_STATISTIC = true
```
## 3 Execute
The config.properties file needs to be located in the same directory with the cbo_planner_test.jar file

```dtd
java -jar cbo_planner_test.jar -f $fe.audit.log.path
```

Analyze SQL Digest and generate reports
```
java -jar cbo_planner_test.jar -f $fe.audit.log.path --benchmark
```

Comparative performance report
```
java -jar cbo_planner_test.jar -f $fe.audit.log.path --cmp --ov old_version_benchmark_file --nv new_version_benchmark_file
```
The benchmark file is the output of --benchmark, and the output can be redirected to a file, 
such as ```java -jar cbo_planner_test.jar -f $fe.audit.log.path --benchmark >new_version_benchmark_file```

## 4 Analysis
The SQL with different execution results or poor performance of the new Planner will be recorded in the result folder