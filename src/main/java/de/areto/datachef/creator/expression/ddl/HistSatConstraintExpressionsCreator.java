package de.areto.datachef.creator.expression.ddl;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.template.ForeignKey;
import de.areto.datachef.model.template.ForeignKeyBuilder;
import de.areto.datachef.model.template.PrimaryKey;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class HistSatConstraintExpressionsCreator implements SQLExpressionQueueCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);
    private final Link link;

    public HistSatConstraintExpressionsCreator(@NonNull Link link) {
        this.link = link;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        if (!link.isHistoricized()) {
            final String msg = "Creation of History Satellites is only permitted for historicized Links";
            throw new CreatorException(msg);
        }

        final Queue<SQLExpression> queue = new LinkedList<>();
        if(dwhConfig.generatePrimaryKeys())
            queue.add(createPrimaryKeyExpression());

        if(dwhConfig.generateForeignKeys())
            queue.add(createForeignKeyExpression());

        return queue;
    }

    private SQLExpression createForeignKeyExpression() throws CreatorException {
        final String tableName = dvConfig.satNamePrefix() + link.getName() + dvConfig.histSatSuffix();
        final String refTableName = link.getNamePrefix() + link.getName();
        final String description = "Foreign Key for History Satellite for " + link.getType() + " " + link.getName();
        final String fkName = dwhConfig.foreignKeyPrefix() + tableName;

        final ForeignKey foreignKey = new ForeignKeyBuilder()
                .setSchemaName(dwhConfig.schemaNameServed())
                .setTableName(tableName)
                .setName(fkName)
                .setTargetSchema(dwhConfig.schemaNameServed())
                .setTargetTable(refTableName)
                .createForeignKey();

        final String column = link.getName() + link.getKeySuffix();
        foreignKey.addColumnReference(column, column);

        final Map<String, Object> context = new HashMap<>();
        context.put("foreignKey", foreignKey);
        context.put("object", link);

        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).createForeignKeyConstraintTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, description);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    private SQLExpression createPrimaryKeyExpression() throws CreatorException {
        final String tableName = dvConfig.satNamePrefix() + link.getName() + dvConfig.histSatSuffix();
        final String pkName = dwhConfig.primaryKeyPrefix() + tableName;
        final PrimaryKey primaryKey = new PrimaryKey(dwhConfig.schemaNameServed(), tableName, pkName);
        primaryKey.addColumn(link.getName() + link.getKeySuffix());
        primaryKey.addColumn(dvConfig.loadDateName());

        final Map<String, Object> context = new HashMap<>();
        context.put("primaryKey", primaryKey);
        context.put("object", link);

        try {
            final String desc = String.format("Primary Key for History Satellite for %s %s", link.getType(), link.getName());
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).createPrimaryKeyConstraintTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }
}
