package edu.cmu.cs.db.calcite_app.app;

import edu.cmu.cs.db.calcite_app.app.utils.Utils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final List<RelOptRule> OPTIMIZATION_RULES = List.of(
            CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
            CoreRules.SORT_REMOVE,
            CoreRules.LIMIT_MERGE,
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.CALC_REMOVE,
            CoreRules.MULTI_JOIN_OPTIMIZE,


            CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
            CoreRules.AGGREGATE_VALUES,
            CoreRules.AGGREGATE_JOIN_REMOVE,
            CoreRules.PROJECT_FILTER_TRANSPOSE,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY,
//            CoreRules.JOIN_COMMUTE,
//            CoreRules.JOIN_TO_MULTI_JOIN,
//            CoreRules.MULTI_JOIN_OPTIMIZE,
//                CoreRules.PROJECT_CALC_MERGE,
//                CoreRules.PROJECT_TO_CALC,
//                CoreRules.CALC_MERGE,

            CoreRules.FILTER_CORRELATE,
            CoreRules.FILTER_MERGE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_CORRELATE,
            CoreRules.FILTER_SCAN,
            CoreRules.FILTER_MULTI_JOIN_MERGE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
            CoreRules.AGGREGATE_FILTER_TRANSPOSE,
            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE,

            CoreRules.PROJECT_CORRELATE_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,

            CoreRules.PROJECT_AGGREGATE_MERGE,
            CoreRules.PROJECT_JOIN_TRANSPOSE,
            CoreRules.PROJECT_SET_OP_TRANSPOSE,

            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,

            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE


            // Stuck:
//            CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,
//            CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
//            CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE
            //            CoreRules.JOIN_TO_MULTI_JOIN,
    );

    private static void processSql(String filename, String sql, Config config
            , Map<String, TableStatistics> statistics) throws SQLException,
            ValidationException, SqlParseException, RelConversionException,
            IOException {
        log.info("Processing {}", filename);

        String caseName = filename.substring(0, filename.lastIndexOf('.'));
        File originalSQLFile = new File(config.getOutputFolder(), filename);
        File optimizedSQLFile = new File(config.getOutputFolder(),
                caseName + "_optimized.sql");
        File logicalPlanFile = new File(config.getOutputFolder(), caseName +
                ".txt");
        File enumerablePlanFile = new File(config.getOutputFolder(),
                caseName + "_optimized.txt");
        File executionResult = new File(config.getOutputFolder(), caseName +
                "_results.csv");

        // Write the original query
        Files.write(originalSQLFile.toPath(), sql.getBytes());

        try (var context = new OptimizationContext(config, statistics)) {
            // Parse the query into relational node
            var relNode = context.parse(sql);
            Files.write(logicalPlanFile.toPath(),
                    Utils.serializePlan(relNode).getBytes());

            // Optimize the relational node
            relNode = context.optimize(relNode);
            var optimizedPlan = Utils.serializePlan(relNode);
            Files.write(enumerablePlanFile.toPath(), optimizedPlan.getBytes());
            log.info("Optimized plan: \n{}", optimizedPlan);

            // Convert the relational node back to query
            Files.write(optimizedSQLFile.toPath(),
                    context.convertBackToSql(relNode).getBytes());

            log.info("Executing the optimized plan");
            FileWriter fileWriter = new FileWriter(executionResult);
            context.executeAndSerialize(relNode, fileWriter);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            log.error("Usage: java -jar App.jar <QueryFolder> <OutputFolder>");
            return;
        }

        var databaseFile = new File("duckdb.db");
        var config = new Config(OPTIMIZATION_RULES, databaseFile.getPath(),
                args[0],
                args[1]);

        if (!databaseFile.exists()) {
            config.loadDataSet();
        }

        config.analyze();
        var stats = new TableStatistics.Builder(config.getDataSource()).build();

        config.addRules(
                JoinPushThroughJoinRule.RIGHT,
                JoinPushThroughJoinRule.LEFT
        );

        // FIXME: q9, q19, q21
        config.addFilteredCase("q9.sql");
        config.addFilteredCase("q19.sql");
        config.addFilteredCase("q21.sql");

        for (var stat : stats.values()) {
            log.info("Table[{}]: {} rows", stat.getTableName(),
                    stat.getRowCount());
        }

        for (var sqlEntry : config.getQueries().entrySet()) {
            var filename = sqlEntry.getKey();
            var query = sqlEntry.getValue();

            processSql(filename, query, config, stats);
        }
    }
}
