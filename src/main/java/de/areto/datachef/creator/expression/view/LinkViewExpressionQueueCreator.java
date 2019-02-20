package de.areto.datachef.creator.expression.view;

import com.google.common.collect.Multimap;
import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.*;
import java.util.stream.Collectors;

public class LinkViewExpressionQueueCreator implements SQLExpressionQueueCreator {

    private final Link link;
    private final Collection<Satellite> satellites;
    private final Multimap<String, Satellite> hubSatelliteMap;
    private final List<String> templates = ConfigCache.getOrCreate(TemplateConfig.class).viewForLinkTemplates();

    public LinkViewExpressionQueueCreator(@NonNull Link link, @NonNull Collection<Satellite> linkSatellites, @NonNull Multimap<String, Satellite> hubSatelliteMap) {
        this.link = link;
        this.satellites = linkSatellites;
        this.hubSatelliteMap = hubSatelliteMap;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        final Queue<SQLExpression> queue = new LinkedList<>();
        final List<Satellite> satsSorted = satellites.stream().sorted().collect(Collectors.toList());

        final Map<String, Object> context = new HashMap<>();
        context.put("object", link);
        context.put("satellites", satsSorted);
        context.put("hubSatMap", hubSatelliteMap);

        final List<String> tplSorted = templates.stream().sorted().collect(Collectors.toList());

        for (int i = 0; i < tplSorted.size(); i++) {
            final String template = tplSorted.get(i);
            final String desc = String.format("View for %s %s #%d", link.getType(), link.getName(), (i + 1));
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
