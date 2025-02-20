package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(), RelOptUtil.dumpPlan("",
                relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet,
                                           File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(", ");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(", ");
                }
                resultSetString.append(resultSet.getString(i));
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    private static CalciteConnection initCalciteConnection() throws SQLException {
        Properties info = new Properties();

        info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName()
                , "false");
        return DriverManager
                .getConnection("jdbc:calcite:", info)
                .unwrap(CalciteConnection.class);
    }

    private static HepPlanner buildHepPlanner(List<RelOptRule> rules) {
        HepProgramBuilder builder = new HepProgramBuilder();
        rules.forEach(builder::addRuleInstance);
        return new HepPlanner(builder.build());
    }

    private static SqlNode convertBackToSql(RelNode relNode) {
        RelToSqlConverter converter =
                new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
        var result = converter.visitInput(relNode, 0);
        return result.asQueryOrValues();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <QueryFolder> " +
                    "<OutputFolder>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as
        // you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);

        // Note: in practice, you would probably use org.apache.calcite.tools
        // .Frameworks.
        // That package provides simple defaults that make it easier to
        // configure Calcite.
        // But there's a lot of magic happening there; since this is an
        // educational project,
        // we guide you towards the explicit method in the writeup.

        var calciteConnection = initCalciteConnection();
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        var dataSource = JdbcSchema.dataSource("jdbc:duckdb:data.db", "org" +
                ".duckdb.DuckDBDriver", null, null);
        log.info("connection schema: " + dataSource.getConnection().getSchema());

        rootSchema.add("duckdb", JdbcSchema.create(
                rootSchema, "duckdb", dataSource, null, "main"
        ));
        calciteConnection.setSchema("duckdb");

        var sql = """
                    SELECT c.c_custkey, c.c_name
                    FROM customer c
                    JOIN orders o1 ON c.c_custkey = o1.o_custkey
                    JOIN orders o2 ON c.c_custkey = o2.o_custkey
                    WHERE o1.o_orderstatus = 'F'
                      AND o2.o_orderpriority = '1-URGENT'
                      AND EXISTS (
                        SELECT 1
                        FROM lineitem l
                        WHERE l.l_orderkey = o1.o_orderkey
                        AND l.l_quantity > 30
                      )
                """;


        var frameworkConfig = Frameworks
                .newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("duckdb"))
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .build();

        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(sql);
        SqlNode sqlNodeValidated = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNodeValidated);
        RelNode relNode = relRoot.project();

        log.error(RelOptUtil.dumpPlan("",
                relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES));

        // Convert the rule to be `Enumerable`.
        var hepPlanner = buildHepPlanner(EnumerableRules.rules());

        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();

        var runner = calciteConnection.unwrap(RelRunner.class);
        try (var stmt = runner.prepareStatement(relNode)) {
            var result = stmt.executeQuery();
            while (result.next()) {
                System.out.println(result.getRow());
            }
        }

        log.error(convertBackToSql(relNode).toString());
    }
}
