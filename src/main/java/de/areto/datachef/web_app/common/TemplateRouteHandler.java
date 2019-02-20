package de.areto.datachef.web_app.common;

import de.areto.common.template.WebTemplateRenderer;
import de.areto.datachef.Application;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public abstract class TemplateRouteHandler extends RouteHandler {

    public abstract Map<String, Object> createContext(Request request, Response response) throws Exception;

    public TemplateRouteHandler() {
        super();
        checkState(!getRouteConfig().getTemplate().equals(WebRoute.DEFAULT_TEMPLATE));
    }

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final Map<String, Object> context = this.createContext(request, response);
        final Map<String, Object> finalContext = getDefaultContext();
        finalContext.putAll(context);
        final String template = this.getRouteConfig().getTemplate();
        return new WebTemplateRenderer(template).render(context);
    }

    private Map<String, Object> getDefaultContext() {
        final Map<String, Object> context = new HashMap<>();
        context.put("def_queue_worker_size", Application.get().getWorkerService().getQueueSize());
        context.put("def_queue_sink_size", Application.get().getSinkService().getQueueSize());
        return context;
    }
}
