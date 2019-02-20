package de.areto.datachef.creator.expression.ddl;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.jdbc.DBTable;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.Collections;
import java.util.Map;

public class CreateTableExpressionCreator implements SQLExpressionCreator {

    private final DBTable table;

    public CreateTableExpressionCreator(@NonNull DBTable table) {
        this.table = table;
    }

    @Override
    public SQLExpression createExpression() throws CreatorException {
        try {
            final Map<String, Object> context = Collections.singletonMap("table", table);
            final String desc = String.format("Create table %s.%s", table.getSchema(), table.getName());
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).createTableTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.CREATE, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }
}
