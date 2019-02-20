package de.areto.datachef.creator.expression.view;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Satellite;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.*;
import java.util.stream.Collectors;

public class HubViewExpressionQueueCreator implements SQLExpressionQueueCreator {

    private final Hub hub;
    private final Collection<Satellite> satellites;
    private final List<String> templates = ConfigCache.getOrCreate(TemplateConfig.class).viewForHubTemplates();

    public HubViewExpressionQueueCreator(@NonNull Hub hub, @NonNull Collection<Satellite> satellites) {
        this.hub = hub;
        this.satellites = satellites;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        final Queue<SQLExpression> queue = new LinkedList<>();
        final List<Satellite> satsSorted = satellites.stream().sorted().collect(Collectors.toList());

        final Map<String, Object> context = new HashMap<>();
        context.put("object", hub);
        context.put("satellites", satsSorted);

        final List<String> tplSorted = templates.stream().sorted().collect(Collectors.toList());

        for (int i = 0; i < tplSorted.size(); i++) {
            final String template = tplSorted.get(i);
            final String desc = String.format("View for %s %s #%d", hub.getType(), hub.getName(), (i + 1));
            try {
                final String sql = new SQLTemplateRenderer(template).render(context);
                queue.add(new SQLExpression(SQLExpression.QueryType.VIEW, sql, desc));
            } catch (RenderingException e) {
                throw new CreatorException(e);
            }
        }

        return queue;
    }
}
