package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

public class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);

    private final Set<String> filteredCases;
    private final List<RelOptRule> rules;
    private final String queryFolder;
    private final String outputFolder;
    private final String databaseFile;

    public Config(List<RelOptRule> rules,
                  String databaseFile,
                  String queryFolder,
                  String outputFolder) {
        this.rules = new ArrayList<>(rules);
        this.queryFolder = queryFolder;
        this.outputFolder = outputFolder;
        this.databaseFile = databaseFile;
        filteredCases = new HashSet<>();
    }

    public DataSource getDataSource() {
        var url = "jdbc:duckdb:" + getDatabaseFile();
        return JdbcSchema.dataSource(
                url, "org.duckdb.DuckDBDriver", null, null
        );
    }

    public void loadDataSet() throws IOException, SQLException {
        var dataPath = Path.of(queryFolder, "..", "data");

        try (var connection = getDataSource().getConnection()) {
            // 1. load schema
            var schemaPath = Path.of(dataPath.toString(), "schema.sql");
            try (var statement =
                         connection.prepareStatement(Files.readString(schemaPath))) {
                statement.execute();
            }


            // 2. populate data
            for (var file : dataPath.toFile().listFiles()) {
                var fileName = file.getName();
                if (fileName.endsWith(".parquet")) {
                    var tableName = fileName.substring(0,
                            fileName.length() - ".parquet".length());
                    var copy = String.format("COPY %s FROM '%s' (FORMAT " +
                            "'parquet')", tableName, file.toPath());
                    try (var statement = connection.prepareStatement(copy)) {
                        statement.execute();
                    }
                }
            }
        }
    }

    public void analyze() throws SQLException {
        try (var connection = getDataSource().getConnection()) {
            try (var statement = connection.prepareStatement("analyze")) {
                statement.execute();
            }
        }
    }

    public void addFilteredCase(String caseName) {
        filteredCases.add(caseName);
    }

    public void addRules(RelOptRule... rules) {
        this.rules.addAll(Arrays.asList(rules));
    }

    public void addRules(Collection<RelOptRule> rules) {
        this.rules.addAll(rules);
    }

    public List<RelOptRule> getRules() {
        return rules;
    }

    public String getQueryFolder() {
        return queryFolder;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public boolean isFilteredCase(String caseName) {
        return filteredCases.contains(caseName);
    }

    public Map<String, String> getQueries() throws IOException {
        if (queryFolder.toLowerCase().endsWith(".sql")) {
            File file = new File(queryFolder);
            return Map.of(queryFolder, Files.readString(file.toPath()));
        }

        File directory = new File(queryFolder);
        Map<String, String> queries = new HashMap<>();

        for (File file : Objects.requireNonNull(directory.listFiles())) {
            var filename = file.getName();
            if (file.toString().toLowerCase().endsWith(".sql")) {
                queries.put(filename, Files.readString(file.toPath()));
            }
        }
        return queries;
    }

    public String getDatabaseFile() {
        return databaseFile;
    }

    public JdbcSchema getJdbcSchema(SchemaPlus rootSchema, String name) {
        var jdbcSchema = JdbcSchema.create(
                rootSchema, name, getDataSource(), null, null
        );
        log.info("connection schema: {}", jdbcSchema.getTableNames());
        return jdbcSchema;
    }
}
