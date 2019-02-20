package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.common.util.LoggingQueue;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

@WebRoute(
        path = "/heartbeat/data",
        responseTransformer = JsonTransformer.class
)
public class HeartbeatDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        return LoggingQueue.MESSAGE_QUEUE;
    }
}
