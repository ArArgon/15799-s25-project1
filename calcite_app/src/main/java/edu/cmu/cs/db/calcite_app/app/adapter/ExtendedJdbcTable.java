package edu.cmu.cs.db.calcite_app.app.adapter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Optional;

public class ExtendedJdbcTable extends AbstractTable implements ScannableTable {
    private final JdbcTable jdbcTable;
    private List<Object[]> enumerableCache;

    public ExtendedJdbcTable(JdbcTable jdbcTable) {
        this.jdbcTable = jdbcTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return jdbcTable.getRowType(typeFactory);
    }

    @Override
    public <C> C unwrapOrThrow(Class<C> aClass) {
        return jdbcTable.unwrapOrThrow(aClass);
    }

    @Override
    public <C> Optional<C> maybeUnwrap(Class<C> aClass) {
        return jdbcTable.maybeUnwrap(aClass);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return jdbcTable.scan(root);
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return jdbcTable.getJdbcTableType();
    }

    @Override
    public String toString() {
        return "ExtendedJdbcTable {" + jdbcTable.tableName() + "}";
    }

    private SqlString cardinalitySql() {
        final SqlNodeList selectList =
                new SqlNodeList(ImmutableList.of(SqlIdentifier.STAR),
                        SqlParserPos.ZERO);
        SqlSelect node =
                new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList,
                        jdbcTable.tableName(), null, null, null, null, null,
                        null, null, null, null);
        final SqlWriterConfig config = SqlPrettyWriter.config()
                .withAlwaysUseParentheses(true)
                .withDialect(jdbcTable.jdbcSchema.dialect);
        final SqlPrettyWriter writer = new SqlPrettyWriter(config);
        node.unparse(writer, 0, 0);
        return writer.toSqlString();
    }

    @Override
    public Statistic getStatistic() {

        // TODO: inject statistics
        return new Statistic() {
            @Override
            public @Nullable Double getRowCount() {
                return Statistic.super.getRowCount();
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }

            @Override
            public @Nullable List<ImmutableBitSet> getKeys() {
                return List.of();
            }
        };
    }
}
