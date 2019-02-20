package de.areto.datachef.creator.expression;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Utility class that provides static methods for the creation of {@link SQLExpression} instances related
 * to tables in the DWH. Creations are stateless and very short wherefore no {@link SQLExpressionCreator}
 * or {@link SQLExpressionQueueCreator} was created instead of these static methods.
 */
@UtilityClass
public class TableExpressionFactory {

    public static SQLExpression truncateExpression(@NonNull String schema, @NonNull String table) throws CreatorException {
        final String desc = String.format("Truncate '%s.%s'", schema, table);
        final Map<String, Object> context = new HashMap<>();
        context.put("schema", schema);
        context.put("table", table);
        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).truncateTableTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.TRUNCATE, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    public static SQLExpression dropTableExpression(@NonNull DVObject object) throws CreatorException {
        final String schema = ConfigCache.getOrCreate(DWHConfig.class).schemaNameServed();
        final String tableName = object.getNamePrefix() + object.getName();
        return dropTableExpression(schema, tableName);
    }

    public static SQLExpression dropTableExpression(@NonNull String schema, @NonNull String table) throws CreatorException {
        final String desc = String.format("Drop table '%s.%s'", schema, table);
        final Map<String, Object> context = new HashMap<>();
        context.put("schema", schema);
        context.put("table", table);
        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).dropTableTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.DROP, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    public static SQLExpression dropViewExpression(@NonNull String schema, @NonNull String viewName) throws CreatorException {
        final String desc = String.format("Drop view '%s.%s'", schema, viewName);
        final Map<String, Object> context = new HashMap<>();
        context.put("schema", schema);
        context.put("view", viewName);
        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).dropViewTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.DROP, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    public static SQLExpression deleteIdsExpression(@NonNull String schema, @NonNull String table, @NonNull List<Long> idList) throws CreatorException {
        checkState(!idList.isEmpty(), "List if ids to delete must not be empty");
        final String desc = String.format("Delete from '%s.%s'", schema, table);
        final Map<String, Object> context = new HashMap<>();
        context.put("schema", schema);
        context.put("table", table);
        context.put("idList", idList);
        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).deleteIdsFromTableTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.DELETE, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

}
