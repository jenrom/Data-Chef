package de.areto.datachef.web_app.handler;


import de.areto.common.watcher.DirWatcherEvent;
import de.areto.datachef.Application;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.extern.slf4j.Slf4j;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static de.areto.datachef.web_app.handler.DeploySearchDataHandler.PATH_CACHE;

@WebRoute(
        path = "/deploy/trigger",
        requestType = HttpMethod.post
)
@Slf4j
public class DeployTriggerHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        final List<String> paths = PATH_CACHE.getIfPresent(request.session().id());

        if(paths == null) log.warn("Deployment ignored; cache expired, start again...");

        for(String pathString : paths) {
            try {
                final Path path = Paths.get(pathString);
                final DirWatcherEvent event = new DirWatcherEvent(DirWatcherEvent.Type.CREATE, path);
                Application.get().getSinkService().queueEvent(event, false);
            } catch (Exception e) {
                log.error("Unable to queue file at path {}", pathString);
            }
        }

        return "";
    }
}
