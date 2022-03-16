// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.planner;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkCmp {
    public static void cmp(String logPath1, String logPath2) throws IOException {
        class TimeCmp {
            Long t1;
            Long t2;

            TimeCmp(Long t1, Long t2) {
                this.t1 = t1;
                this.t2 = t2;
            }
        }

        Map<String, TimeCmp> cmp = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(logPath1))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\t");
                cmp.put(s[0], new TimeCmp(Long.parseLong(s[4]), null));
            }
        }

        try (BufferedReader br = new BufferedReader(new FileReader(logPath2))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\t");
                if (cmp.get(s[0]) != null) {
                    cmp.get(s[0]).t2 = Long.parseLong(s[4]);
                }
            }
        }
        System.out.println("SQL DIGEST" + "\t\t\t" + "Original(ms)" + "\t" + "New(ms)" + "\t" + "Performance boost");

        for (Map.Entry<String, TimeCmp> m : cmp.entrySet()) {
            System.out.println(m.getKey() + "\t"
                    + m.getValue().t1 + "\t"
                    + m.getValue().t2 + "\t"
                    + m.getValue().t1 / m.getValue().t2);
        }
    }
}
