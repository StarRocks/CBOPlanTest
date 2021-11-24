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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dbunit.JdbcBasedDBTestCase;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;

public class StarrocksUtils extends JdbcBasedDBTestCase {
    private static final Logger LOGGER = LogManager.getLogger(StarrocksUtils.class);

    protected static Connection connection;
    protected static IDatabaseConnection databaseConnection;

    protected static String httpAuth;
    protected static String httpUrl;
    protected static OkHttpClient client = new OkHttpClient();
    private static final String STATS_DB = "_statistics_";
    private static final String STATS_TABLE = "table_statistic_v1";

    public void init() {
        try {
            // connection create
            databaseConnection = getConnection();
            databaseConnection.getConfig()
                    .setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new MySqlDataTypeFactory());
            connection = databaseConnection.getConnection();

            httpAuth = "Basic " + Base64.getEncoder().encodeToString((Config.USER + ":" + Config.PASS).getBytes());
            httpUrl = "http://" + Config.IP + ":" + Config.HTTP_PORT + "/query_profile?query_id=";

            enableProfile();
            if (Config.FRAGMENT_INSTANCE > 0) {
                setParallelFragmentExecInstance(Config.FRAGMENT_INSTANCE);
            }
        } catch (Exception e) {
            LOGGER.error("get connection failed", e);
        }
    }

    public String getConnectionUrl() {
        return Config.CONNECT_URL;
    }

    public String getDriverClass() {
        return Config.DRIVER_CLASS;
    }

    public String getUsername() {
        return Config.USER;
    }

    public String getPassword() {
        return Config.PASS;
    }

    // starrocks don't need test dataset
    public IDataSet getDataSet() throws Exception {
        return new CachedDataSet();
    }

    public static void useDb(String dbName) throws Exception {
        connection.createStatement().executeQuery("use " + dbName + ";");
    }

    public static void enableNewPlanner() throws Exception {
        connection.createStatement().executeQuery("set enable_cbo = true;");
    }

    public static void disableNewPlanner() throws Exception {
        connection.createStatement().executeQuery("set enable_cbo = false;");
    }

    public static void enableProfile() throws Exception {
        LOGGER.info("set is_report_success = true;");
        connection.createStatement().executeQuery("set is_report_success = true;");
    }

    public static void setParallelFragmentExecInstance(int n) throws SQLException {
        LOGGER.info("set parallel_fragment_exec_instance_num = " + n + ";");
        connection.createStatement().executeQuery("set parallel_fragment_exec_instance_num = " + n + ";");
    }

    public static Pair<ITable, Long> query(String sql) throws Exception {
        long start = System.currentTimeMillis();
        ITable result = databaseConnection.createQueryTable("RESULT", sql);
        return Pair.of(result, (System.currentTimeMillis() - start));
    }

    public static String explain(String sql, String model) throws SQLException, DataSetException {
        ITable explain = databaseConnection.createQueryTable("EXPLAIN", "explain " + model + " " + sql);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < explain.getRowCount(); i++) {
            sb.append(explain.getValue(i, "EXPLAIN STRING").toString()).append("\n");
        }

        return sb.toString();
    }

    public static String lastQueryID() throws Exception {
        ITable result = databaseConnection.createQueryTable("1", "select last_query_id() as query;");
        return result.getValue(0, "query").toString();
    }

    public static String profile(String queryID) throws IOException {
        Request request = new Request.Builder()
                .url(httpUrl + queryID)
                .header("Authorization", httpAuth)
                .build();

        Response response = client.newCall(request).execute();

        if (!response.isSuccessful() || null == response.body()) {
            throw new IOException(response.message());
        }

        String data = response.body().string();

        int start = data.indexOf("  Summary:");
        int end = data.indexOf("</pre></div></body></html>");
        return data.substring(start, end);
    }

    public static void exportStatistics() throws Exception {
        if (!Config.COLLECT_RESULT_DATA) {
            return;
        }
        String queryStats = "select * from " + STATS_DB + "." + STATS_TABLE;
        ITable statsTable = databaseConnection.createQueryTable("STATS", queryStats);
        writeToCSV(statsTable);
    }

    private static void writeToCSV(ITable table) throws Exception {
        BufferedWriter out = new BufferedWriter(new FileWriter(Config.OUTPUT_DIR + "/stats.csv"));
        int columnCount = table.getTableMetaData().getColumns().length;
        String[] columns = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = table.getTableMetaData().getColumns()[i].getColumnName();
        }
        for (int i = 0; i < table.getRowCount(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < columns.length; j++) {
                Object value = table.getValue(i, columns[j]);
                DataType dataType = table.getTableMetaData().getColumns()[j].getDataType();
                if (value == null) {
                    sb.append("NULL");
                } else if (dataType.isDateTime()) {
                    String stringValue = DataType.asString(value);
                    sb.append(stringValue, 0, stringValue.length() - 2);
                } else {
                    String stringValue = DataType.asString(value);
                    sb.append(stringValue);
                }
                sb.append("\t");
            }
            if (sb.length() != 0) {
                sb.delete(sb.length() - 1, sb.length());
            }
            sb.append("\n");
            out.write(sb.toString());
        }
        out.flush();
        out.close();
    }
}
