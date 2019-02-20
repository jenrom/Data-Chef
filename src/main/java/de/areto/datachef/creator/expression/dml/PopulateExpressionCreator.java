package de.areto.datachef.creator.expression.dml;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.mapping.Mapping;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class PopulateExpressionCreator<T extends DVObject> implements SQLExpressionCreator {

    private final Mapping mapping;
    private final T object;
    private final String template;

    @Getter @Setter
    private String role;

    public PopulateExpressionCreator(@NonNull Mapping mapping, @NonNull T object, @NonNull String template) {
        checkState(!template.isEmpty(), "Provide at least one template");
        this.mapping = mapping;
        this.object = object;
        this.template = template;
    }

    @Override
    public SQLExpression createExpression () throws CreatorException{
        final Map<String, Object> context = new HashMap<>();
        context.put("object", object);
        context.put("mapping", mapping);
        context.put("role", role);

        try {
            final String desc = String.format("Populate %s %s for Mapping '%s'",
                    object.getType(), object.getName(), mapping.getName());
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.POPULATE, sql, desc);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

}
