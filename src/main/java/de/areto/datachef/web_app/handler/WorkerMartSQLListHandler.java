package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;


@WebRoute(
        path = "/worker/mart/sql/list",
        template = "worker_mart_list.vm"
)
public class WorkerMartSQLListHandler extends TemplateRouteHandler{
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "mart-worker");
        return context;
    }
}
