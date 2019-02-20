package de.areto.datachef.creator;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.creator.expression.ddl.CreateTableExpressionCreator;
import de.areto.datachef.creator.table.MartTableCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartColumn;
import de.areto.datachef.model.template.PrimaryKey;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class MartDDLQueueCreator implements SQLExpressionQueueCreator {

    private final Mart mart;

    private DBTable martTable;

    public MartDDLQueueCreator(@NonNull Mart mart) {
        this.mart = mart;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        Queue<SQLExpression> queue = new LinkedList<>();
        queue.add(createTableExpression());
        queue.add(createPrimaryKeyExpression());

        return queue;
    }

    private SQLExpression createTableExpression() throws CreatorException {
        MartTableCreator martTableCreator = new MartTableCreator(mart);
        martTable = martTableCreator.createDbTable();
        return new CreateTableExpressionCreator(martTable).createExpression();
    }

    private SQLExpression createPrimaryKeyExpression() throws CreatorException {
        final String schemaName = martTable.getSchema();
        final String tableName = martTable.getName();
        final String pkName = ConfigCache.getOrCreate(DWHConfig.class).primaryKeyPrefix() + tableName;
        final PrimaryKey primaryKey = new PrimaryKey(schemaName, tableName, pkName);

        for (MartColumn martColumn : mart.getColumns()) {
            if (martColumn.isIdentityColumn() || martColumn.isKeyColumn())
                primaryKey.addColumn(martColumn.getName());
        }

        final Map<String, Object> context = new HashMap<>();
        context.put("primaryKey", primaryKey);

        final String desc = String.format("Primary Key for Mart %s", mart.getName());
        final String template = ConfigCache.getOrCreate(TemplateConfig.class).createPrimaryKeyConstraintTemplate();

        try {
            final String sql  = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, desc);
        } catch (RenderingException e) {
            final String msg = String.format("Error rendering template '%s'", template);
            throw new CreatorException(msg, e);
        }
    }
}
