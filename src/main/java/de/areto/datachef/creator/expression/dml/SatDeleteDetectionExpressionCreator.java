package de.areto.datachef.creator.expression.dml;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class SatDeleteDetectionExpressionCreator implements SQLExpressionCreator {

    private final Satellite satellite;
    private final Mapping mapping;

    public SatDeleteDetectionExpressionCreator(@NonNull Satellite satellite, @NonNull Mapping mapping) {
        checkState(mapping.isFullLoad(), "Full load must be enabled");
        this.satellite = satellite;
        this.mapping = mapping;
    }

    @Override
    public SQLExpression createExpression() throws CreatorException {
        final Map<String, Object> context = new HashMap<>();
        context.put("object", satellite);
        context.put("mapping", mapping);

        final String description = String.format("Delete detection on %s %s for Mapping '%s'",
                satellite.getType(), satellite.getName(), mapping.getName());

        final String template = ConfigCache.getOrCreate(TemplateConfig.class).satDeleteDetectionOnFullLoadTemplate();

        try {
            final String sql = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.POPULATE, sql, description);
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }
}
