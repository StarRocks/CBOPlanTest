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

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dbunit.DatabaseUnitException;
import org.dbunit.assertion.DbUnitAssert;
import org.dbunit.assertion.DefaultFailureHandler;
import org.dbunit.assertion.Difference;
import org.dbunit.assertion.FailureHandler;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.AbstractDataType;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

class StarrocksUnitAssert extends DbUnitAssert {
    private static final Logger LOG = LogManager.getLogger(StarrocksUnitAssert.class);

    private static final DoubleTypeIgnorePrecision DOUBLE_TYPE =
            new DoubleTypeIgnorePrecision("DoubleIgnorePrecision", Types.DOUBLE);

    private static final List<String> RANDOM_FUNCTIONS = Lists.newArrayList("rand()", "random()",
            "current_time()", "localtime()", "localtimestamp()", "now()", "unix_timestamp()", "utc_timestamp()");

    public void assertWithValueComparer(final ITable expectedTable,
                                        final ITable actualTable, final FailureHandler failureHandler,
                                        final ValueComparer defaultValueComparer,
                                        final Map<String, ValueComparer> columnValueComparers)
            throws DatabaseUnitException {

        // Do not continue if same instance
        if (expectedTable == actualTable) {
            LOG.debug("The given tables reference the same object."
                    + " Skipping comparisons.");
            return;
        }

        final FailureHandler validFailureHandler = determineFailureHandler(new ExceptionHandler());

        final ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        final ITableMetaData actualMetaData = actualTable.getTableMetaData();
        final String expectedTableName = expectedMetaData.getTableName();

        final boolean isTablesEmpty =
                compareRowCounts(expectedTable, actualTable, validFailureHandler, expectedTableName);
        if (isTablesEmpty) {
            return;
        }

        // Put the columns into the same order
        final Column[] expectedColumns = Columns.getSortedColumns(expectedMetaData);
        final Column[] actualColumns = Columns.getSortedColumns(actualMetaData);

        // Verify columns
        //compareColumns(expectedColumns, actualColumns, expectedMetaData,
        //        actualMetaData, validFailureHandler);

        // Get the datatypes to be used for comparing the sorted columns
        final ComparisonColumn[] comparisonCols =
                getComparisonColumns(expectedTableName, expectedColumns, actualColumns, validFailureHandler);

        // Finally compare the data
        compareData(expectedTable, actualTable, comparisonCols, validFailureHandler, defaultValueComparer,
                columnValueComparers);
    }

    protected void compareData(ITable expectedTable, ITable actualTable, ComparisonColumn[] comparisonCols,
                               FailureHandler failureHandler, ValueComparer defaultValueComparer,
                               Map<String, ValueComparer> columnValueComparers, int rowNum, int columnNum)
            throws DatabaseUnitException {
        ComparisonColumn compareColumn = comparisonCols[columnNum];
        String columnName = compareColumn.getColumnName();
        DataType dataType = compareColumn.getDataType();

        Object expectedValue = expectedTable.getValue(rowNum, columnName);
        Object actualValue = actualTable.getValue(rowNum, columnName);

        if (DataType.DOUBLE.equals(dataType) || DataType.FLOAT.equals(dataType)) {
            dataType = DOUBLE_TYPE;
        }

        if (this.skipCompare(columnName, expectedValue, actualValue)) {
            LOG.trace("skipCompare: ignoring comparison {}={} on column={}", expectedValue, actualValue,
                    columnName);
        } else {
            ValueComparer valueComparer =
                    this.determineValueComparer(columnName, defaultValueComparer, columnValueComparers);
            LOG.debug("compareData: comparing actualValue={} to expectedValue={} with valueComparer={}",
                    actualValue, expectedValue, valueComparer);
            String failMessage = valueComparer
                    .compare(expectedTable, actualTable, rowNum, columnName, dataType, expectedValue, actualValue);
            this.failIfNecessary(expectedTable, actualTable, failureHandler, rowNum, columnName, expectedValue,
                    actualValue, failMessage);
        }
    }

    @Override
    protected boolean skipCompare(String columnName, Object expectedValue, Object actualValue) {
        String c = columnName.toLowerCase();
        return RANDOM_FUNCTIONS.stream().anyMatch(c::contains);
    }

    private static class DoubleTypeIgnorePrecision extends AbstractDataType {
        /**
         * Logger for this class
         */
        DoubleTypeIgnorePrecision(String name, int sqlType) {
            super(name, sqlType, Double.class, true);
        }

        public Object typeCast(Object value) throws TypeCastException {
            return DataType.DOUBLE.typeCast(value);
        }

        public Object getSqlValue(int column, ResultSet resultSet)
                throws SQLException, TypeCastException {
            return DataType.DOUBLE.getSqlValue(column, resultSet);
        }

        public void setSqlValue(Object value, int column, PreparedStatement statement)
                throws SQLException, TypeCastException {
            DataType.DOUBLE.setSqlValue(value, column, statement);
        }

        @Override
        protected int compareNonNulls(Object value1, Object value2) throws TypeCastException {
            double v1 = (double) value1;
            double v2 = (double) value2;

            LOG.trace("double1: {}, double2: {}", v1, v2);
            int re = super.compareNonNulls(value1, value2);

            if (re == 0) {
                return 0;
            }

            // 1.0 precision ignore
            if (v1 - v2 < 1 || v1 - v2 < -1) {
                return 0;
            }

            return re;
        }
    }

    private static class ExceptionHandler extends DefaultFailureHandler {
        @Override
        public void handle(Difference diff) {
            final String msg = buildMessage(diff);

            final AssertRowError err =
                    new AssertRowError(msg, String.valueOf(diff.getExpectedValue()),
                            String.valueOf(diff.getActualValue()));

            err.setDifference(diff);
            throw err;
        }
    }
}