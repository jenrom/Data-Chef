package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

@WebRoute(
        path = "/develop/messages",
        responseTransformer = JsonTransformer.class
)
public class DevelopMessagesDataHandler extends RouteHandler {
    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String sessionId = request.session().id();
        final Mapping mapping = DevelopCallbackHandler.MAPPING_CACHE.getIfPresent(sessionId);

        if(mapping == null) {
            final String msg = String.format("No simulated Mapping for '%s' available", sessionId);
            throw new WebException(msg);
        }

        return mapping.getIssueList();
    }
}
