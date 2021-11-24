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

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.dbunit.assertion.DbUnitAssert;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.junit.Assert;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SQLDiffer {
    private static final DbUnitAssert EQUALS_INSTANCE = new StarrocksUnitAssert();

    private static final java.text.DecimalFormat numberFormat = new java.text.DecimalFormat("0.00");

    private final String db;

    private final String sql;

    private final SQLog sqlLog;

    public SQLDiffer(String db, String sql) {
        this.db = db;
        this.sql = sql;
        this.sqlLog = new SQLog(db, sql);
    }

    public SQLog validate() {
        try {
            if (StringUtils.isNotBlank(db)) {
                StarrocksUtils.useDb(db);
            }

            validateResult(false);
        } catch (MySQLSyntaxErrorException e) {
            collectError(SQLog.ErrorType.SYNTAX, e, 1);
        } catch (Throwable e) {
            // check again
            try {
                collectError(SQLog.ErrorType.OTHER, e, 1);
                validateResult(true);
            } catch (MySQLSyntaxErrorException exception) {
                collectError(SQLog.ErrorType.SYNTAX, e, 2);
            } catch (Throwable exception) {
                collectError(SQLog.ErrorType.OTHER, exception, 2);
            }
        }

        return sqlLog;
    }

    private void validateResult(boolean withLog) throws Exception {
        StarrocksUtils.disableNewPlanner();
        Pair<ITable, Long> oldResult = StarrocksUtils.query(sql);
        sqlLog.setOldQueryId(StarrocksUtils.lastQueryID());

        StarrocksUtils.enableNewPlanner();
        Pair<ITable, Long> newResult = StarrocksUtils.query(sql);
        sqlLog.setNewQueryId(StarrocksUtils.lastQueryID());

        ITable newData = newResult.getLeft();
        ITable oldData = oldResult.getLeft();

        try {
            Assert.assertEquals(oldData.getRowCount(), newData.getRowCount());
        } catch (Throwable e) {
            if (!withLog) {
                throw e;
            }

            collectError(SQLog.ErrorType.ROW_COUNT, e, 2);
            collectDataLimit(oldData, newData);
            collectExplain();
            collectProfile();
            return;
        }

        String lowerSql = sql.toLowerCase();
        if (lowerSql.contains("limit") && !lowerSql.contains("order by")) {
            logTimeCost(oldResult.getRight(), newResult.getRight());
            return;
        }

        ITable sortedExpected = new SortedTable(newData, newData.getTableMetaData().getColumns());
        ITable sortedActual = new SortedTable(oldData, oldData.getTableMetaData().getColumns());
        try {
            EQUALS_INSTANCE.assertEquals(sortedExpected, sortedActual);
        } catch (Throwable e) {
            if (!withLog) {
                throw e;
            }

            collectError(SQLog.ErrorType.ROW_DIFF, e, 2);
            if (e instanceof AssertRowError) {
                collectErrorData(oldData, newData, (AssertRowError) e);
            } else {
                collectDataLimit(oldData, newData);
            }
            collectExplain();
            collectProfile();
            return;
        }

        logTimeCost(oldResult.getRight(), newResult.getRight());
    }

    private void collectExplain() {
        try {
            StarrocksUtils.enableNewPlanner();
            sqlLog.setNewExplain(StarrocksUtils.explain(sql, "costs"));
        } catch (Exception e) {
            sqlLog.setNewExplain(formatError(e));
        }

        try {
            StarrocksUtils.disableNewPlanner();
            sqlLog.setOldExplain(StarrocksUtils.explain(sql, "verbose"));
        } catch (Exception e) {
            sqlLog.setOldExplain(formatError(e));
        }
    }

    private void collectError(SQLog.ErrorType type, Throwable e, int nums) {
        sqlLog.setType(type);
        sqlLog.setMsg((sqlLog.getMsg() + "\nNo." + nums + " ERROR:\n" + formatError(e)).trim());
    }

    private void collectDataLimit(ITable oldTable, ITable newTable) {
        if (!Config.COLLECT_RESULT_DATA) {
            return;
        }
        sqlLog.setOldData(formatData(oldTable, 0, Math.min(oldTable.getRowCount(), 1000)));
        sqlLog.setNewData(formatData(newTable, 0, Math.min(newTable.getRowCount(), 1000)));
    }

    private void collectErrorData(ITable oldTable, ITable newTable, AssertRowError e) {
        if (!Config.COLLECT_RESULT_DATA) {
            return;
        }
        if (null != e && null != e.getDifference()) {
            int row = e.getDifference().getRowIndex();
            sqlLog.setOldData(
                    formatData(oldTable, Math.max(0, row - 500), Math.min(oldTable.getRowCount(), row + 500)));
            sqlLog.setNewData(
                    formatData(newTable, Math.max(0, row - 500), Math.min(newTable.getRowCount(), row + 500)));
        }
    }

    private String formatError(Throwable e) {
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private String formatData(ITable table, int start, int end) {
        try {
            StringBuilder data = new StringBuilder();
            for (Column column : table.getTableMetaData().getColumns()) {
                for (int i = start; i < end; i++) {
                    data.append(table.getValue(i, column.getColumnName()).toString());
                    data.append("\t");
                }
                data.append("\n");
            }

            return data.toString();
        } catch (DataSetException e) {
            return formatError(e);
        }
    }

    private void collectProfile() {
        try {
            StarrocksUtils.disableNewPlanner();
            sqlLog.setOldProfile(StarrocksUtils.profile(sqlLog.getOldQueryId()));
        } catch (Exception e) {
            sqlLog.setOldProfile(formatError(e));
        }

        try {
            StarrocksUtils.enableNewPlanner();
            sqlLog.setNewProfile(StarrocksUtils.profile(sqlLog.getNewQueryId()));
        } catch (Exception e) {
            sqlLog.setNewProfile(formatError(e));
        }
    }

    private void logTimeCost(Long oldTime, Long newTime) {
        double radio = newTime * 1.0 / oldTime;
        long diff = newTime - oldTime;
        if ((newTime < 300 && oldTime < 300) || (diff < 300 && radio < 1.5)) {
            sqlLog.setType(SQLog.ErrorType.SUCCESS);
            sqlLog.setMsg("ignore, new: " + newTime + "ms, old: " + oldTime + "ms, radio: " + numberFormat
                    .format(radio));
        } else if (newTime > oldTime) {
            sqlLog.setType(SQLog.ErrorType.SLOW);
        } else if (newTime < oldTime) {
            sqlLog.setType(SQLog.ErrorType.SUCCESS);
            sqlLog.setMsg("fast, new: " + newTime + "ms, old: " + oldTime + "ms, radio: " + numberFormat
                    .format(radio));
        }

        if (!sqlLog.success()) {
            if (Config.SLOW_PROFILE) {
                collectProfile();
            }
            if (Config.SLOW_EXPLAIN) {
                collectExplain();
            }
            sqlLog.setMsg("slow, new: " + newTime + "ms, old: " + oldTime + "ms, radio: " + numberFormat
                    .format(radio));
        }
    }
}
