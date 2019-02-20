package de.areto.datachef.model.template;

public class ForeignKeyBuilder {
    private String schemaName;
    private String tableName;
    private String name;
    private String targetSchema;
    private String targetTable;

    public ForeignKeyBuilder setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public ForeignKeyBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ForeignKeyBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public ForeignKeyBuilder setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
        return this;
    }

    public ForeignKeyBuilder setTargetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public ForeignKey createForeignKey() {
        return new ForeignKey(schemaName, tableName, name, targetSchema, targetTable);
    }
}