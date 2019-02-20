package de.areto.datachef.creator.expression.ddl;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.template.PrimaryKey;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.Map;

public class PrimaryKeyExpressionCreator implements SQLExpressionCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    private final DVObject object;

    public PrimaryKeyExpressionCreator(DVObject object) {
        this.object = object;
    }

    @Override
    public SQLExpression createExpression() throws CreatorException {
        final String tableName = object.getNamePrefix() + object.getName();
        final String pkName = dwhConfig.primaryKeyPrefix() + tableName;
        final PrimaryKey primaryKey = new PrimaryKey(dwhConfig.schemaNameServed(), tableName, pkName);

        if(object instanceof Satellite) {
            final Satellite sat = (Satellite) object;
            final String satKeyName = sat.getParent().getName() + sat.getParent().getKeySuffix();
            primaryKey.addColumn(satKeyName);
            primaryKey.addColumn(dvConfig.loadDateName());
        } else {
            final String keyName = object.getName() + object.getKeySuffix();
            primaryKey.addColumn(keyName);
        }

        final Map<String, Object> context = new HashMap<>();
        context.put("primaryKey", primaryKey);
        context.put("object", object);

        try {
            final String desc = String.format("Primary Key for %s %s", object.getType(), object.getName());
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).createPrimaryKeyConstraintTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CONSTRAINT, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }
}
