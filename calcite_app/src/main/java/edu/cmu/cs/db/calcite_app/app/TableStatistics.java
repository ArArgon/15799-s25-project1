package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.schema.Statistic;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableStatistics implements Statistic {
    private final String tableName;
    private final Map<String, Integer> approxUnique;
    private final Map<String, Integer> count;
    private final Map<String, Integer> ordinals;

    private TableStatistics(String tableName,
                            Map<String, Integer> approxUnique,
                            Map<String, Integer> count,
                            Map<String, Integer> ordinals) {
        this.tableName = tableName;
        this.approxUnique = approxUnique;
        this.count = count;
        this.ordinals = ordinals;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Integer> getApproxUnique() {
        return approxUnique;
    }

    public Map<String, Integer> getCount() {
        return count;
    }

    public Map<String, Integer> getOrdinals() {
        return ordinals;
    }

    @Override
    public @Nullable Double getRowCount() {
        for (Map.Entry<String, Integer> entry : count.entrySet()) {
            return (double) entry.getValue();
        }

        return (double) 0;
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return Statistic.super.getDistribution();
    }

    public static class Builder {
        private final DataSource dataSource;

        public Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        private List<String> getTableNames() throws SQLException {
            List<String> tableNames = new ArrayList<>();

            try (Connection connection = dataSource.getConnection()) {
                var metadata = connection.getMetaData();
                try (var tables = metadata.getTables(null, null, "%", null)) {
                    while (tables.next()) {
                        tableNames.add(tables.getString("TABLE_NAME"));
                    }
                }
            }

            return tableNames;
        }

        private TableStatistics getTableStatistics(String tableName) throws SQLException {
            Map<String, Integer> approxUniqueMap = new HashMap<>();
            Map<String, Integer> countMap = new HashMap<>();
            Map<String, Integer> ordinalMap = new HashMap<>();

            var statQuery = """
                    select column_name, approx_unique, count
                    from (summarize %s)
                    """;
            var catalogQuery = """
                    select column_name, ordinal_position
                    from information_schema.columns
                    where table_name = ?
                    """;

            try (var connection = dataSource.getConnection()) {
                var sql = String.format(statQuery, tableName);
                try (var statStmt = connection.prepareStatement(sql)) {
                    try (var resultSet = statStmt.executeQuery()) {
                        while (resultSet.next()) {
                            var columnName = resultSet.getString("column_name");
                            var approxUnique = resultSet.getInt(
                                    "approx_unique");
                            var count = resultSet.getInt("count");

                            approxUniqueMap.put(columnName, approxUnique);
                            countMap.put(columnName, count);
                        }
                    }
                }

                try (var statStmt = connection.prepareStatement(catalogQuery)) {
                    statStmt.setString(1, tableName);
                    try (var resultSet = statStmt.executeQuery()) {
                        while (resultSet.next()) {
                            var columnName = resultSet.getString("column_name");
                            var ordinal = resultSet.getInt("ordinal_position");
                            ordinalMap.put(columnName, ordinal);
                        }
                    }
                }
            }

            return new TableStatistics(tableName, approxUniqueMap, countMap,
                    ordinalMap);
        }

        public Map<String, TableStatistics> build() throws SQLException {
            Map<String, TableStatistics> result = new HashMap<>();
            for (var tableName : getTableNames()) {
                result.put(tableName, getTableStatistics(tableName));
            }
            return result;
        }
    }

}
