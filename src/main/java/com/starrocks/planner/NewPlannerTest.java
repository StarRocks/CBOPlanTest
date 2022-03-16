/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.planner;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class NewPlannerTest {
    private static final Logger LOGGER = LogManager.getLogger(NewPlannerTest.class);

    private static FileWriter replayWriter = null;

    public static void init() {
        File file = new File(Config.OUTPUT_DIR + "/" + Config.REPLAY_LOG);
        try {
            replayWriter = new FileWriter(file);
            LOGGER.info("mkdir log file {}", file.toString());
        } catch (IOException e) {
            LOGGER.error("Init replay file failed. ", e);
        }
    }

    private static void readAuditLog(String path) throws Exception {
        File configFile = new File(path);
        if (!configFile.exists()) {
            LOGGER.error("Couldn't find the fe.audit.log file");
        }

        LOGGER.info("read file start");
        int count = 0;
        int error = 0;
        int lineNum = 0;

        DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String line;
            StringBuilder auditBuilder = new StringBuilder();
            boolean isStart;
            while ((line = br.readLine()) != null) {
                lineNum++;
                isStart = line.contains("query] |Client");

                if (!isStart) {
                    auditBuilder.append(line).append(" ");
                    continue;
                }

                if (auditBuilder.length() == 0) {
                    auditBuilder.append(line).append(" ");
                    continue;
                }

                String auditLog = auditBuilder.toString();
                Optional<Pair<String, String>> sql = getSQL(auditLog);
                if (!sql.isPresent()) {
                    auditBuilder = new StringBuilder();
                    continue;
                }

                LocalDateTime time = LocalDateTime.parse(auditLog.substring(0, auditLog.indexOf(",")), DATE_TIME_FORMATTER);
                if (time.getHour() < 8) {
                    continue;
                }

                try {
                    SQLDiffer differ = new SQLDiffer(sql.get().getLeft(), sql.get().getRight());
                    SQLog log = differ.validate();
                    log.setId(lineNum);
                    log.write();

                    if (!log.success()) {
                        writeReplayLog(auditLog);
                    }

                    error += log.success() ? 0 : 1;
                } catch (Exception e) {
                    LOGGER.warn("diff audit exception. line: {} ", auditLog, e);
                    error++;
                }

                count++;
                if (count % 1000 == 0) {
                    LOGGER.info("test sql {}, error {}.", count, error);
                }

                // clear audit
                auditBuilder = new StringBuilder();
            }
        }

        LOGGER.info("read file end");
    }

    private static void writeReplayLog(String line) throws IOException {
        if (null != replayWriter) {
            replayWriter.append(line).append("\n");
        }
    }

    private static Optional<Pair<String, String>> getSQL(String auditLog) {
        String[] strings = auditLog.split("\\|");
        if (strings.length < 14) {
            return Optional.empty();
        }

        boolean isQuery = Boolean.parseBoolean(strings[11].split("=")[1]);
        String sql = strings[13].split("Stmt=")[1];
        String lowSQL = sql.toLowerCase().trim();
        if (isQuery
                && !lowSQL.contains("@@")
                && !lowSQL.startsWith("explain")
                && lowSQL.contains("from")
                && !(strings[4].equals("State=ERR"))) {

            String db = "";
            if (strings[3].length() > 3) {
                db = strings[3].split("=")[1].split(":")[1];
            }

            // split use
            if (lowSQL.startsWith("use ")) {
                int firstSemicolon = sql.indexOf(";");
                String useDb = sql.substring(0, firstSemicolon).trim();
                db = useDb.substring(useDb.lastIndexOf(" ")).trim();
                sql = sql.substring(firstSemicolon + 1);
            }

            return Optional.of(Pair.of(db, sql.trim()));
        }

        return Optional.empty();
    }

    public static void main(String[] args) throws ParseException, IOException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("benchmark", false, "Print benchmark information");
        options.addOption("diff", false, "Diff result with new and old planner");
        options.addOption("f", "file", true, "Audit file path");
        options.addOption("cmp", false, "Performance comparison");
        options.addOption("ov", true, "Performance comparison");
        options.addOption("nv", true, "Performance comparison");

        CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption("benchmark")) {
            BenchmarkTest.bench(commandLine.getOptionValue("file"));
        } else if (commandLine.hasOption("cmp")){
            BenchmarkCmp.cmp(commandLine.getOptionValue("ov"), commandLine.getOptionValue("nv"));
        } else if (commandLine.hasOption('d')) {
            try {
                Thread.sleep(5000);
                Config.init();
                SQLog.init();
                NewPlannerTest.init();

                LOGGER.info("config init done");
                String logPath = args[0];

                new StarrocksUtils().init();
                LOGGER.info("connection init done");
                StarrocksUtils.exportStatistics();
                readAuditLog(logPath);
            } catch (Throwable e) {
                LOGGER.error("error : " + e.getMessage(), e);
            }
        }
    }
}
