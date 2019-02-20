package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/develop",
        template = "develop.vm"
)
public class DevelopHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "develop");
        context.put("code", request.session().attribute("code"));
        return context;
    }
}
