package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.Application;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.worker.MappingWorkerCargo;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import de.areto.datachef.worker.RollbackMappingWorker;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.Optional;

@WebRoute(
        path = "/mapping/rollback/:dbId",
        responseTransformer = JsonTransformer.class
)
public class RollbackMappingHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String idString = request.params("dbId");

        if(idString == null || idString.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final long dbId = Long.valueOf(idString);
            final Optional<MappingWorkerCargo> mappingCargo = session.byId(MappingWorkerCargo.class).loadOptional(dbId);

            if(!mappingCargo.isPresent()) {
                throw new WebException(String.format("Mapping '%d' not found", dbId));
            }

            final RollbackMappingWorker rollbackWorker = new RollbackMappingWorker(mappingCargo.get().getName());
            Application.get().getWorkerService().execute(rollbackWorker);

            return rollbackWorker.getWorkerInfo().getRunId();
        }
    }
}