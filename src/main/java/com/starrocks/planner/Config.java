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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Config {
    private static final Logger logger = LogManager.getLogger(Config.class);

    public static String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static String CONNECT_URL = "jdbc:mysql://127.0.0.1:8112/test";
    public static String USER = "root";
    public static String PASS = "123";
    public static String IP = "127.0.0.1";
    public static int HTTP_PORT = 8110;
    public static boolean COLLECT_RESULT_DATA = false;
    public static boolean COLLECT_STATISTIC = false;

    public static String OUTPUT_DIR = "output/result";
    public static String REPLAY_LOG = "replay.log";

    public static int FRAGMENT_INSTANCE = -1;

    public static boolean SLOW_PROFILE = true;
    public static boolean SLOW_EXPLAIN = true;

    public static void init() {
        try {
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                Properties properties = new Properties();
                properties.load(new FileInputStream(configFile));
                CONNECT_URL = properties.getProperty("CONNECT_URL");
                USER = properties.getProperty("USER");
                PASS = properties.getProperty("PASS");
                HTTP_PORT = Integer.parseInt(properties.getProperty("HTTP_PORT").trim());

                if (properties.containsKey("COLLECT_RESULT_DATA")) {
                    COLLECT_RESULT_DATA = Boolean.parseBoolean(properties.getProperty("COLLECT_RESULT_DATA"));
                }

                if (properties.containsKey("COLLECT_STATISTIC")) {
                    COLLECT_STATISTIC = Boolean.parseBoolean(properties.getProperty("COLLECT_STATISTIC"));
                }

                if (properties.containsKey("SLOW_PROFILE")) {
                    SLOW_PROFILE = Boolean.parseBoolean(properties.getProperty("SLOW_PROFILE"));
                }

                if (properties.containsKey("SLOW_EXPLAIN")) {
                    SLOW_EXPLAIN = Boolean.parseBoolean(properties.getProperty("SLOW_EXPLAIN"));
                }

                if (properties.containsKey("FRAGMENT_INSTANCE")) {
                    FRAGMENT_INSTANCE = Integer.parseInt(properties.getProperty("FRAGMENT_INSTANCE"));
                }
            } else {
                logger.error("Not find config file!");
                System.exit(-1);
            }

            File outputFile = new File(OUTPUT_DIR);
            if (outputFile.exists()) {
                FileUtils.cleanDirectory(outputFile);
            } else {
                FileUtils.forceMkdir(outputFile);
            }

            LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
            File file = new File("log4j2.xml");
            context.setConfigLocation(file.toURI());
        } catch (Exception e) {
            logger.error("init failed", e);
        }

        Pattern p = Pattern.compile("(\\d+\\.)+\\d+");
        Matcher matcher = p.matcher(CONNECT_URL);

        if (matcher.find()) {
            IP = matcher.group();
        } else {
            logger.error("Not find IP in connect url");
        }
    }
}
