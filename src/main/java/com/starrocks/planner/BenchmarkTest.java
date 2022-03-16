// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.planner;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BenchmarkTest {
    private static final Logger LOGGER = LogManager.getLogger(BenchmarkTest.class);

    private static class TimeAndCount {
        public TimeAndCount(Long time) {
            this.avgTime = time;
            this.minTime = time;
            this.maxTime = time;
            this.count++;
        }

        Long avgTime = 0L;
        Long maxTime = Long.MIN_VALUE;
        Long minTime = Long.MAX_VALUE;
        Long count = 0L;

        public void touch(Long time) {
            if (time > maxTime) {
                maxTime = time;
            }
            if (time < minTime) {
                minTime = time;
            }

            avgTime = (avgTime * count + time) / (count + 1);
            count++;
        }

        public Long getAvgTime() {
            return avgTime;
        }

        public Long getMaxTime() {
            return maxTime;
        }

        public Long getMinTime() {
            return minTime;
        }

        public Long getCount() {
            return count;
        }
    }


    public static void bench(String logPath) throws IOException {
        Map<String, TimeAndCount> digestMap = new HashMap<>();

        File auditFile = new File(logPath);
        if (!auditFile.exists()) {
            LOGGER.error("Couldn't find the fe.audit.log file");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(auditFile))) {
            String line;
            StringBuilder auditBuilder = new StringBuilder();
            boolean isStart;
            while ((line = br.readLine()) != null) {
                isStart = line.contains("query] |Client");
                if (isStart) {
                    String auditLog = auditBuilder.toString();
                    refresh(digestMap, auditLog);
                    auditBuilder = new StringBuilder(line);
                } else {
                    auditBuilder.append(line).append(" ");
                }
            }
            refresh(digestMap, auditBuilder.toString());
        }

        System.out.println("SQL DIGEST" + "\t\t\t" + "SQL count" + "\t" + "Max time(ms)" + "\t" + "Min time(ms)" + "\t" + "Avg Time(ms)");
        for (Map.Entry<String, TimeAndCount> d : digestMap.entrySet()) {
            TimeAndCount tc = d.getValue();
            System.out.println(d.getKey() + "\t" +
                    tc.getCount() + "\t" +
                    tc.getMaxTime() + "\t" +
                    tc.getMinTime() + "\t" +
                    tc.getAvgTime());
        }
    }

    private static void refresh(Map<String, TimeAndCount> digestMap, String auditLog) {
        if (auditLog.isEmpty()) {
            return;
        }

        String[] strings = auditLog.split("\\|");
        boolean isQuery = Boolean.parseBoolean(strings[11].split("=")[1]);
        if (strings[14].split("Digest=").length > 1) {
            String digest = strings[14].split("Digest=")[1];
            if (isQuery && !digest.trim().isEmpty() && !(strings[4].equals("State=ERR"))) {
                Long time = Long.parseLong(strings[5].split("Time=")[1]);

                if (digestMap.get(digest) != null) {
                    digestMap.get(digest).touch(time);
                } else {
                    digestMap.put(digest, new TimeAndCount(time));
                }
            }
        }
    }
}
