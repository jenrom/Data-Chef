package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.Collections;
import java.util.Map;

@WebRoute(
        path = "/deploy",
        template = "deploy.vm"
)
public class DeployHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        return Collections.singletonMap("active", "deploy");
    }
}
