package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/rollbacks/list",
        template = "rollbacks_list.vm"
)
public class RollbacksListHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "rollbacks");
        return context;
    }
}
