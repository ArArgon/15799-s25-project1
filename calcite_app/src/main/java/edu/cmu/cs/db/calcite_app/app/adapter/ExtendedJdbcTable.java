package edu.cmu.cs.db.calcite_app.app.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Optional;

public class ExtendedJdbcTable extends AbstractTable implements ScannableTable {
    private final JdbcTable jdbcTable;

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
        // TODO: explore the potential caching opportunity
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

    @Override
    public Statistic getStatistic() {
        return super.getStatistic();
    }
}
