package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;


@WebRoute(
        path = "/worker/data/sql/list",
        template = "worker_data_sql_list.vm"
)
public class WorkerDataSQLListHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "worker-data-sql");
        return context;
    }
}
