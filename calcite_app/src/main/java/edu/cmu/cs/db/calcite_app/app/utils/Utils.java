package edu.cmu.cs.db.calcite_app.app.utils;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Utils {
    private Utils() {
    }

    public static RelNode enumerableLimitReplacer(RelNode input) {
        if (input instanceof EnumerableLimit limit) {
            var inner = limit.getInputs().get(0);
            if (inner instanceof EnumerableSort sort) {
                return new EnumerableLimitSort(input.getCluster(),
                        input.getTraitSet(), sort.getInput(),
                        sort.getCollation(), limit.offset,
                        limit.fetch);
            }
            return new EnumerableLimitSort(input.getCluster(),
                    input.getTraitSet(), inner.getInput(0),
                    RelCollations.of(), limit.offset,
                    limit.fetch);
        } else {
            for (var i = 0; i < input.getInputs().size(); i++) {
                input.replaceInput(i,
                        enumerableLimitReplacer(input.getInput(i)));
            }
        }
        return input;
    }

    public static void printTraits(RelNode relNode) {
        System.out.println("Visit RelNode: " + relNode);
        for (var trait : relNode.getTraitSet()) {
            System.out.println("\t" + trait.toString());
        }

        for (var input : relNode.getInputs()) {
            printTraits(input);
        }
    }


    public static String serializePlan(RelNode relNode) {
        return RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
    }

    public static void serializeResultSet(ResultSet resultSet,
                                          Writer writer) throws SQLException,
            IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                writer.write(",");
            }
            writer.write(metaData.getColumnName(i));
        }
        writer.write("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    writer.write(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                writer.write("\"");
                writer.write(s);
                writer.write("\"");
            }
            writer.write("\n");
        }
        writer.flush();
    }
}
