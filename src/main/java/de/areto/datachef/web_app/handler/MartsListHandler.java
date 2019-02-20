package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/marts/list",
        template = "marts_list.vm"
)

public class MartsListHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "marts");
        return context;
    }
}
