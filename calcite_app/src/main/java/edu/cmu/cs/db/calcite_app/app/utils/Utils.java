package edu.cmu.cs.db.calcite_app.app.utils;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;

public class Utils {
    private Utils() {
    }

    public static RelNode EnumerableLimitReplacer(RelNode input) {
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
        }
        return input;
    }
}
