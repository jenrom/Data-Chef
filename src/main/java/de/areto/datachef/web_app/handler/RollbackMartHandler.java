package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.Application;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.worker.MartWorkerCargo;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import de.areto.datachef.worker.RollbackMartWorker;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.Optional;

@WebRoute(
        path = "/mart/rollback/:dbId",
        responseTransformer = JsonTransformer.class
)
public class RollbackMartHandler extends RouteHandler {
    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String idString = request.params("dbId");

        if(idString == null || idString.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final long dbId = Long.valueOf(idString);
            final Optional<MartWorkerCargo> martCargo = session.byId(MartWorkerCargo.class).loadOptional(dbId);

            if(!martCargo.isPresent()) {
                throw new WebException(String.format("Mart '%d' not found", dbId));
            }

            final RollbackMartWorker rollbackWorker = new RollbackMartWorker(martCargo.get().getName());
            Application.get().getWorkerService().execute(rollbackWorker);

            return rollbackWorker.getWorkerInfo().getRunId();
        }
    }
}
