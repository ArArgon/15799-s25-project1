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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Config {
    private static final String DUCKDB_SCHEMA = "duckdb";
    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private final List<RelOptRule> rules;
    private final String queryFolder;
    private final String outputFolder;
    private final String databaseFile;

    public Config(List<RelOptRule> rules,
                  String databaseFile,
                  String queryFolder,
                  String outputFolder) {
        this.rules = rules;
        this.queryFolder = queryFolder;
        this.outputFolder = outputFolder;
        this.databaseFile = databaseFile;
    }

    private DataSource getDataSource() {
        var url = "jdbc:duckdb:" + getDatabaseFile();
        return JdbcSchema.dataSource(
                url, "org.duckdb.DuckDBDriver", null, null
        );
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

    public Map<String, String> getQueries() throws IOException {
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

    public JdbcSchema getJdbcSchema(SchemaPlus rootSchema) {
        var jdbcSchema = JdbcSchema.create(
                rootSchema, DUCKDB_SCHEMA, getDataSource(), null, null
        );
        log.info("connection schema: " + jdbcSchema.getTableNames());
        return jdbcSchema;
    }
}
