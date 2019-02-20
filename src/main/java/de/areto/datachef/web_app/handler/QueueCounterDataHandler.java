package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.Application;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

@WebRoute(
        path = "/qc",
        responseTransformer = JsonTransformer.class
)
public class QueueCounterDataHandler extends RouteHandler {
    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final Object[] res = new Object[2];
        res[0] = Application.get().getWorkerService().getQueueSize();
        res[1] = Application.get().getSinkService().getQueueSize();
        return res;
    }
}
