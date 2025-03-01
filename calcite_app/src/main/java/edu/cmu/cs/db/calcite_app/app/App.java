package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

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

            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_AGGREGATE_MERGE,
            CoreRules.PROJECT_JOIN_TRANSPOSE,
            CoreRules.PROJECT_SET_OP_TRANSPOSE,

            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE


            // Stuck:
//                CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE
//                CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
//                CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE
    );

    private static void processSql(String sql, Config config) throws SQLException,
            ValidationException, SqlParseException, RelConversionException {
        try (var context = new OptimizationContext(config)) {
            var relNode = context.parse(sql);

//                log.info(Utils.serializePlan(relNode));
            relNode = context.optimize(relNode);

//                log.info(Utils.serializePlan(relNode));
//            log.info("Converted back to Sql: " + context.convertBackToSql
//            (relNode));

//                StringWriter writer = new StringWriter();
//                context.executeAndSerialize(relNode, writer);
//                log.info(writer.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java -jar App.jar <QueryFolder> " +
                    "<OutputFolder>");
            return;
        }

        // FIXME: q19, q21, q22

        var config = new Config(OPTIMIZATION_RULES, "duckdb.db", args[0],
                args[1]);
        for (var sqlEntry : config.getQueries().entrySet()) {
            var filename = sqlEntry.getKey();
            var query = sqlEntry.getValue();

            log.info("Visiting {}", filename);
            processSql(query, config);
        }
    }
}
