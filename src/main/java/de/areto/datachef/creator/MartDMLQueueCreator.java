package de.areto.datachef.creator;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartType;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class MartDMLQueueCreator implements SQLExpressionQueueCreator {

    private final Mart mart;

    public MartDMLQueueCreator(@NonNull Mart mart) {
        this.mart = mart;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        Queue<SQLExpression> queue = new LinkedList<>();
        queue.add(createExpression());

        return queue;
    }

    private SQLExpression createExpression() throws CreatorException{

        final Map<String, Object> context = new HashMap<>();
        context.put("mart", mart);

        final String template;
        final String desc;

        if(mart.getMartType().equals(MartType.LOAD)){
            desc = String.format("Load Data for Mart %s", mart.getName());
            template = ConfigCache.getOrCreate(TemplateConfig.class).martLoadTemplate();
        } else if(mart.getMartType().equals(MartType.RELOAD)){
            desc = String.format("Reload Data for Mart %s", mart.getName());
            template = ConfigCache.getOrCreate(TemplateConfig.class).martReloadTemplate();
        } else if(mart.getMartType().equals(MartType.HISTORICAL)){
            desc = String.format("Historicize Data for Mart %s", mart.getName());
            template = ConfigCache.getOrCreate(TemplateConfig.class).martHistoricalTemplate();
        } else {
            desc = String.format("Merge Data for Mart %s", mart.getName());
            template = ConfigCache.getOrCreate(TemplateConfig.class).martMergedTemplate();
        }

        try {
            final String sql  = new SQLTemplateRenderer(template).render(context);
            return new SQLExpression(SQLExpression.QueryType.POPULATE, sql, desc);
        } catch (RenderingException e) {
            final String msg = String.format("Error rendering template '%s'", template);
            throw new CreatorException(msg, e);
        }
    }
}