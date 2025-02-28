package edu.cmu.cs.db.calcite_app.app.adapter;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ExtendedJdbcSchema implements Schema {
    private final JdbcSchema jdbcSchema;
    private final Map<String, Table> tableMap = new HashMap<>();

    public ExtendedJdbcSchema(JdbcSchema jdbcSchema) {
        this.jdbcSchema = jdbcSchema;
    }
    
    @Override
    public @Nullable Table getTable(String name) {
        if (tableMap.containsKey(name)) {
            return tableMap.get(name);
        }

        var jdbcTable = (JdbcTable) jdbcSchema.getTable(name);
        if (jdbcTable == null) {
            return null;
        }

        var table = new ExtendedJdbcTable(jdbcTable);
        tableMap.put(name, table);
        return table;
    }

    @Override
    public Set<String> getTableNames() {
        return jdbcSchema.getTableNames();
    }

    @Override
    public @Nullable RelProtoDataType getType(String name) {
        return jdbcSchema.getType(name);
    }

    @Override
    public Set<String> getTypeNames() {
        return jdbcSchema.getTypeNames();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return jdbcSchema.getFunctions(name);
    }

    @Override
    public Set<String> getFunctionNames() {
        return jdbcSchema.getFunctionNames();
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
        var schema = jdbcSchema.getSubSchema(name);
        if (schema instanceof JdbcSchema subSchema) {
            return new ExtendedJdbcSchema(subSchema);
        }

        return schema;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return jdbcSchema.getSubSchemaNames();
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema,
                                    String name) {
        return jdbcSchema.getExpression(parentSchema, name);
    }

    @Override
    public boolean isMutable() {
        return jdbcSchema.isMutable();
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        var snapshot = jdbcSchema.snapshot(version);

        return new ExtendedJdbcSchema((JdbcSchema) snapshot);
    }
}
