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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class SQLog {
    private static final Logger LOGGER = LogManager.getLogger(SQLog.class);

    private static final AtomicLong QUERY_ID = new AtomicLong();

    enum ErrorType {
        SUCCESS,
        SLOW,
        ROW_COUNT,
        ROW_DIFF,
        SYNTAX,
        OTHER,
    }

    private ErrorType type;

    private long id;

    private final String db;

    private final String sql;

    private String newQueryId = "";

    private String oldQueryId = "";

    private String msg = "";

    private String newData = "";

    private String oldData = "";

    private String newExplain = "";

    private String oldExplain = "";

    private String newProfile = "";

    private String oldProfile = "";

    public SQLog(String db, String sql) {
        this.db = db;
        this.sql = sql;
        this.type = ErrorType.SUCCESS;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setType(ErrorType type) {
        this.type = type;
    }

    public String getNewQueryId() {
        return newQueryId;
    }

    public void setNewQueryId(String newQueryId) {
        this.newQueryId = newQueryId;
    }

    public String getOldQueryId() {
        return oldQueryId;
    }

    public void setOldQueryId(String oldQueryId) {
        this.oldQueryId = oldQueryId;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    public void setNewData(String newData) {
        this.newData = newData;
    }

    public void setOldData(String oldData) {
        this.oldData = oldData;
    }

    public void setNewExplain(String newExplain) {
        this.newExplain = newExplain;
    }

    public void setOldExplain(String oldExplain) {
        this.oldExplain = oldExplain;
    }

    public void setNewProfile(String newProfile) {
        this.newProfile = newProfile;
    }

    public void setOldProfile(String oldProfile) {
        this.oldProfile = oldProfile;
    }

    public boolean success() {
        return type == ErrorType.SUCCESS;
    }

    public static void init() throws IOException {
        for (int i = 1; i < ErrorType.values().length; i++) {
            File file = new File(Config.OUTPUT_DIR + "/" + ErrorType.values()[i].toString().toLowerCase());
            FileUtils.forceMkdir(file);
        }
    }

    public void write() throws Exception {
        if (success()) {
            LOGGER.info("{} | {} | {}", db, sql, msg);
            return;
        }

        long query = QUERY_ID.addAndGet(1);
        String dir = Config.OUTPUT_DIR + "/" + type.toString().toLowerCase() + "/";
        String sqlFile = dir + query + "_sql";

        try (FileWriter fw = new FileWriter(new File(sqlFile))) {
            fw.append("use ").append(db).append(";\n");
            fw.append(sql);
            fw.append("\nLine: ").append(String.valueOf(id)).append(", ERROR: ");
            fw.append(msg).append("\n");
        }

        if (type == ErrorType.SYNTAX) {
            return;
        }

        String neFile = dir + query + "_new_plain";
        String oeFile = dir + query + "_old_plain";

        String npFile = dir + query + "_new_profile";
        String opFile = dir + query + "_old_profile";

        writeFile(neFile, newExplain);
        writeFile(oeFile, oldExplain);

        writeFile(npFile, newProfile);
        writeFile(opFile, oldProfile);

        if (type == ErrorType.SLOW) {
            return;
        }

        String ndFile = dir + query + "_new_data";
        String odFile = dir + query + "_old_data";

        writeFile(ndFile, newData);
        writeFile(odFile, oldData);
    }

    private void writeFile(String file, String content) throws IOException {
        if (StringUtils.isBlank(content)) {
            return;
        }

        try (FileWriter fw = new FileWriter(new File(file))) {
            fw.write(content);
        }
    }

    @Override
    public String toString() {
        return "SQLog{" +
                "type=" + type +
                ", id=" + id +
                ", db='" + db + '\'' +
                ", sql='" + sql + '\'' +
                ", newQueryId='" + newQueryId + '\'' +
                ", oldQueryId='" + oldQueryId + '\'' +
                ", msg='" + msg + '\'' +
                ", newData='" + StringUtils.isNotBlank(newData) + '\'' +
                ", oldData='" + StringUtils.isNotBlank(oldData) + '\'' +
                ", newExplain='" + StringUtils.isNotBlank(newExplain) + '\'' +
                ", oldExplain='" + StringUtils.isNotBlank(oldExplain) + '\'' +
                ", newProfile='" + StringUtils.isNotBlank(newProfile) + '\'' +
                ", oldProfile='" + StringUtils.isNotBlank(oldProfile) + '\'' +
                '}';
    }
}
