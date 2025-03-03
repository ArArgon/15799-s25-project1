# Project 1 - Query Optimizer Evaluation

1. `JdbcSchema` and `JdbcTable` are okay for `RelRunner` but do not produce enumerable table scans.
2. `JdbcToEnumerableConverterRule` is broken.
   Although it inserts an enumerable shim operator, it cannot handle correlated columns. However, the existing `scan`
   method in a `JdbcTable` is functional, so a wrapper class which delegates invocation would make it enumerable.
3. What prevents `JdbcTable` from generating ordinary logical operator is that it implements `TranslatableTable` which
   causes it to generable special physical operators.
4. Overriding `public Schema snapshot(SchemaVersion version)` in the schema is necessary to hook table retrieval.
   Otherwise, if is delegated to `JdbcSchema`, it would generate a `JdbcTable`. `Schema.snapshot()` is called because
   each optimization is evaluated in a separate transaction.
5. To resolve the 'column not found' problem, set `defaultSchema` in the `FrameworkConfig` to the schema that contains
   the table.
6. Apache Calcite has an abstraction leak. When converting `SqlNode` to `RelNode` (using `Planner.rel`), it requires a
   `RelOptCluster`, and the `Planner` would initialize a `VolcanoPlanner` with default rules. This has unexpected effect
   during optimization: a separate `VolcanoPlanner` would refuse to run because it does belong to the cluster; a
   `HepPlanner` would execute the default `VolcanoPlanner` and ignoring the rules.
7. Using `program.run()` to run the optimizer is better than calling `planner.findBestExp()` by hand, as it would
   initialize the planner and clean up the cluster for you.
8. Surprisingly, `RelRunner.prepareStatement()` will quietly optimize the plan again. It will have unwanted effects as
   sometimes, a plan cannot be optimized twice. Although it is possible to run the optimization once by only letting
   `RelRunner.prepareStatement()` to do the heavy lifting, it would introduce other new problems in other stages.
9. The support for `EnumerableLimit` is broken. It would prevent the `RelRunner` to run the plan. Instead, I converted
   all `EnumerableLimit` operator to `EnumerableLimitSort`.
10. To obtain analytics for each table, one should run `analyze` at the very beginning and inject all statistics into
    our extended `JdbcTable`. Deferring the lookup to `JdbcTable.getStatistic()` does not work because there seems to be
    a deadlock when using JDBC connection in it.
11. When converting a `RelNode` back to SQL string, Apache Calcite always prepends a schema access, i.e.
    `select * from duckdb.customer`. I could not turn it off by tuning configurations. In the end, I strip them using a
    regular expression.
12. `Calc` rules are broken in several ways. 1) The optimizer would freeze when `PROJECT_TO_CALC` and
    `PROJECT_CALC_MERGE`
    are simultaneously turned on; 2) `Calc`-family rules would magically fail the second optimization at `RelRunner`,
    it somehow converts enumerable operators back to logical ones.
13. `JOIN_ASSOCIATE` rule is malfunctioning. When turned on, it would trigger an exception complaining
    `EnumerableNestedLoopJoin` cannot be converted because it is 'a physical operator'. It doesn't make any sense to me.
14. `JOIN_COMMUTE` rule would produce a terrible plan to `p4.sql`.
15. `JOIN_PROJECT_{BOTH,LEFT,RIGHT}_TRANSPOSE` would freeze the optimizer.
16. `MULTI_JOIN_OPTIMIZE_BUSHY` actually produces decent join orders.
17. Subqueries are poorly supported by the optimizer and must be unnested. To do so, I first turned on `withExpand` in
    `SqlToRelConverter`'s configuration, and added `PROJECT_SUB_QUERY_TO_CORRELATE` to de-correlate. 