package de.areto.datachef.creator.expression.ddl;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Leg;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.template.ForeignKey;
import de.areto.datachef.model.template.ForeignKeyBuilder;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;

public class ForeignKeyExpressionQueueCreator implements SQLExpressionQueueCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final String template = ConfigCache.getOrCreate(TemplateConfig.class).createForeignKeyConstraintTemplate();
    private final DVObject object;

    public ForeignKeyExpressionQueueCreator(@NonNull DVObject object) {
        final boolean isNotHub = object.isSatellite() || object.isLink();
        checkState(isNotHub, "DVObject must be a Satellite or a Link");
        this.object = object;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        final Queue<SQLExpression> queue = new LinkedList<>();

        if(object.isSatellite())
            queue.add(createSatelliteForeignKey(object.asSatellite()));

        if(object.isLink()) {
            final Link link = object.asLink();
            for(Leg leg : link.getLegsSorted()) {
                queue.add(createLegForeignKey(leg, link.isSelfReference()));
            }
        }

        return queue;
    }

    private SQLExpression createLegForeignKey(@NonNull Leg leg, boolean selfReference) throws CreatorException {
        final Link parentLink = leg.getParentLink();
        final String tableName = parentLink.getNamePrefix() + parentLink.getName();
        final String refTableName = leg.getHub().getNamePrefix() + leg.getHub().getName();
        final String roleSuffix = leg.isDefaultRole() ? "" : "in role " + leg.getRole();
        final String description = String.format("Foreign Key for %s %s%s", leg.getHub().getType(),
                leg.getHub().getName(), roleSuffix);

        String fkName = dwhConfig.foreignKeyPrefix() + tableName + "_leg_" + leg.getHubName();
        if(!leg.isDefaultRole()) fkName += "_" + leg.getRole();

        final ForeignKey foreignKey = new ForeignKeyBuilder()
                .setSchemaName(dwhConfig.schemaNameServed())
                .setTableName(tableName)
                .setName(fkName)
                .setTargetSchema(dwhConfig.schemaNameServed())
                .setTargetTable(refTableName)
                .createForeignKey();

        // Append role name only if Link is a self reference between the same hubs and leg's role is not the default
        // role. Link's column will be renamed according to the role.
        final String hubColName = leg.getHub().getName() + leg.getHub().getKeySuffix();
        final String linkColName = !leg.isDefaultRole() && selfReference ? hubColName + "_" + leg.getRole() : hubColName;
        foreignKey.addColumnReference(linkColName, hubColName);

        try {
            final Map<String, Object> context = new HashMap<>();
            context.put("foreignKey", foreignKey);
            context.put("object", leg);

            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, description);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    private SQLExpression createSatelliteForeignKey(@NonNull Satellite sat) throws CreatorException {
        final String tableName = sat.getNamePrefix() + sat.getName();
        final String refTableName = sat.getParent().getNamePrefix() + sat.getParent().getName();
        final String description = "Foreign Key for " + sat.getType() + " " + sat.getName();
        final String fkName = dwhConfig.foreignKeyPrefix() + tableName;

        final ForeignKey foreignKey = new ForeignKeyBuilder()
                .setSchemaName(dwhConfig.schemaNameServed())
                .setTableName(tableName)
                .setName(fkName)
                .setTargetSchema(dwhConfig.schemaNameServed())
                .setTargetTable(refTableName)
                .createForeignKey();

        final String column = sat.getParent().getName() + sat.getParent().getKeySuffix();
        foreignKey.addColumnReference(column, column);

        final Map<String, Object> context = new HashMap<>();
        context.put("foreignKey", foreignKey);
        context.put("object", sat);

        try {
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, description);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }
}
