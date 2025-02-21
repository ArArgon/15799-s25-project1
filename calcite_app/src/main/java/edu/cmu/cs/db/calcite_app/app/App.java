package edu.cmu.cs.db.calcite_app.app;

import edu.cmu.cs.db.calcite_app.app.adapter.ExtendedJdbcSchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final String DUCKDB_SCHEMA = "duckdb";

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
                resultSetString.append(",");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                resultSetString.append("\"");
                resultSetString.append(s);
                resultSetString.append("\"");
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

    private static void displayTraits(RelNode relNode) {
        System.out.println("Visit RelNode: " + relNode);
        for (var trait : relNode.getTraitSet()) {
            System.out.println("\t" + trait.toString());
        }

        for (var input : relNode.getInputs()) {
            displayTraits(input);
        }
    }


    private static @Nullable JdbcConvention findJdbcConvention(RelNode relNode) {
        for (var input : relNode.getInputs()) {
            if (input.getConvention() instanceof JdbcConvention result) {
                return result;
            }
            var convention = findJdbcConvention(input);
            if (convention != null) {
                return convention;
            }
        }
        return null;
    }

    private static SqlNode parseSql(String sql) throws Exception {
        var parserConfig = SqlParser.config()
                .withCaseSensitive(false);

        return SqlParser.create(sql, parserConfig).parseStmt();
    }

    private static Prepare.CatalogReader initCatalogReader(CalciteSchema rootSchema,
                                                           CalciteConnection connection,
                                                           RelDataTypeFactory typeFactory) {
        return new CalciteCatalogReader(
                rootSchema,
                List.of(DUCKDB_SCHEMA),
                typeFactory,
                connection.config()
        );
    }

    private static RelOptCluster createCluster(RelDataTypeFactory typeFactory) {
        RelOptPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.EMPTY_CONTEXT
        );

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static SqlToRelConverter buildSqlToRelConverter(RelDataTypeFactory typeFactory,
                                                            SqlValidator validator,
                                                            Prepare.CatalogReader catalogReader) {
        var cluster = createCluster(typeFactory);
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        return new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    private static RelRoot toRel(SqlNode sqlNode,
                                 Prepare.CatalogReader catalogReader,
                                 RelDataTypeFactory typeFactory) {

        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(
                List.of(SqlStdOperatorTable.instance())
        );

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                validatorConfig
        );

        // Validate the SQL node
        sqlNode = validator.validate(sqlNode);

        // Build the SQL-to-Rel convertor
        var converter = buildSqlToRelConverter(typeFactory, validator,
                catalogReader);
        return converter.convertQuery(sqlNode, false, true);
    }

    private static RelNode optimize(
            RelOptPlanner planner,
            RelNode node,
            RelTraitSet requiredTraitSet,
            RuleSet rules
    ) {
        Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(
                planner,
                node,
                requiredTraitSet,
                List.of(),
                List.of()
        );
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

        var schema = new ExtendedJdbcSchema(JdbcSchema.create(
                rootSchema, "duckdb", dataSource, null, "main"
        ));

        rootSchema.add("duckdb", schema);
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
                .ruleSets(RuleSets.ofList(EnumerableRules.rules()))
                .build();

        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlNode = planner.parse(sql);
        SqlNode sqlNodeValidated = planner.validate(sqlNode);
        RelRoot relRoot = planner.rel(sqlNodeValidated);
        RelNode relNode = relRoot.project();

        // Convert the rule to be `Enumerable`.
        var cluster = relNode.getCluster();
        var optPlanner = cluster.getPlanner();
        var convention = findJdbcConvention(relNode);
        assert convention != null;

        var rules = new ArrayList();
        rules.addAll(EnumerableRules.rules());
        rules.add(CoreRules.PROJECT_TO_CALC);

        RelTraitSet traits = RelTraitSet.createEmpty();
        traits.plus(ConventionTraitDef.INSTANCE.getDefault());
        traits.plus(EnumerableConvention.INSTANCE);

        displayTraits(relNode);

        relNode = optimize(optPlanner, relNode, traits, RuleSets.ofList
                (rules));

        log.error(RelOptUtil.dumpPlan("",
                relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES));

        displayTraits(relNode);

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
