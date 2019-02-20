package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.Application;
import de.areto.datachef.model.worker.WorkerInfo;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.Data;
import spark.Request;
import spark.Response;

import java.util.List;

@WebRoute(
        path = "/worker/queue/data",
        responseTransformer = JsonTransformer.class
)
public class WorkerQueueDataHandler extends RouteHandler {

    @Data
    public static class Payload {
        private final List<WorkerInfo> data;
    }

    @Override
    public Object doWork(Request request, Response response) {
        final List<WorkerInfo> queueSnapshot = Application.get().getWorkerService().getQueueSnapshot();
        return new Payload(queueSnapshot);

        /* Debugging
        List<WorkerInfo> list = new ArrayList<>();
        list.add(new WorkerInfo(UUID.randomUUID(), MappingWorker.class, "test_country", "test_country.sink", StagingMode.FILE, LocalDateTime.now()));
        list.add(new WorkerInfo(UUID.randomUUID(), MappingWorker.class, "test_subregion", "test_subregion.sink", StagingMode.FILE, LocalDateTime.now()));
        list.add(new WorkerInfo(UUID.randomUUID(), MappingDataFileWorker.class, "test_subregion", "test_subregion.csv", StagingMode.FILE, LocalDateTime.now()));
        list.add(new WorkerInfo(UUID.randomUUID(), MappingDataSQLWorker.class, "test_me", null, StagingMode.FILE, LocalDateTime.now()));
        */
    }
}
