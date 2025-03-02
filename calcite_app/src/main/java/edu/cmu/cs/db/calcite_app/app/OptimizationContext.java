package edu.cmu.cs.db.calcite_app.app;

import edu.cmu.cs.db.calcite_app.app.adapter.ExtendedJdbcSchema;
import edu.cmu.cs.db.calcite_app.app.utils.Utils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OptimizationContext implements AutoCloseable {
    private static final Logger log =
            LoggerFactory.getLogger(OptimizationContext.class);
    private static final String DUCKDB_SCHEMA = "duckdb";

    private final Config config;
    private final CalciteConnection calciteConnection;
    private final Planner planner;


    public OptimizationContext(Config config,
                               Map<String, TableStatistics> statistics) throws SQLException {
        this.calciteConnection = initCalciteConnection();
        this.config = config;

        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        var schema = new ExtendedJdbcSchema(config.getJdbcSchema(rootSchema),
                statistics);

        rootSchema.add(DUCKDB_SCHEMA, schema);
        calciteConnection.setSchema(DUCKDB_SCHEMA);

        var frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema(DUCKDB_SCHEMA))
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .sqlToRelConverterConfig(SqlToRelConverter.config().withExpand(true))
                .build();

        planner = Frameworks.getPlanner(frameworkConfig);
    }

    private static CalciteConnection initCalciteConnection() throws SQLException {
        Properties info = new Properties();

        info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName()
                , "false");
        return DriverManager.getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
    }

    private RelNode postOptimize(RelNode rel) {
        rel = Utils.enumerableLimitReplacer(rel);

        return rel;
    }

    public RelNode parse(String sql) throws ValidationException,
            SqlParseException, RelConversionException {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode sqlNodeValidated = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNodeValidated);
        return relRoot.project();
    }

    public RelNode optimize(RelNode rel) {
        var cluster = rel.getCluster();
        var optPlanner = cluster.getPlanner();
        List<RelOptRule> rules = new ArrayList<>(config.getRules());
        rules.addAll(EnumerableRules.rules());

        RelTraitSet traits = RelTraitSet.createEmpty();
        for (var trait : rel.getTraitSet()) {
            traits = traits.plus(trait);
        }

        traits = traits.plus(ConventionTraitDef.INSTANCE.getDefault());
        traits = traits.plus(EnumerableConvention.INSTANCE);

        Program program = Programs.of(RuleSets.ofList(rules));

        rel = program.run(
                optPlanner,
                rel,
                traits,
                List.of(),
                List.of()
        );

        return postOptimize(rel);
    }

    public String convertBackToSql(RelNode rel) {
        RelToSqlConverter converter =
                new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);

        var result = converter.visitRoot(rel);
        return result
                .asQueryOrValues()
                .toSqlString(PostgresqlSqlDialect.DEFAULT)
                .getSql();
    }

    public void executeAndSerialize(RelNode rel, Writer writer) throws SQLException,
            IOException {

        var runner = calciteConnection.unwrap(RelRunner.class);
        try (var stmt = runner.prepareStatement(rel)) {
            Utils.serializeResultSet(stmt.executeQuery(), writer);
        }
    }

    @Override
    public void close() throws SQLException {
        calciteConnection.close();
    }
}
