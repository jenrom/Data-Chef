package de.areto.datachef.web_app.handler;

import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/",
        template = "dashboard.vm"
)
public class DashboardHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final Map<String, Object> context = new HashMap<>();
        context.put("active", "dashboard");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Long countMappingOkay = session.createQuery("select count(*) from MappingWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();
            context.put("countMappingOkay", countMappingOkay);

            final Long countMartOkay = session.createQuery("select count(*) from MartWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();
            context.put("countMartOkay", countMartOkay);

            final Long countDataWorker = session.createQuery("select count(*) from DataWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();

            context.put("countDataWorkerOkay", countDataWorker);

            final Long countMappingDataFileWorker = session.createQuery("select count(*) from MappingDataFileWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();

            context.put("countMappingDataFileWorkerOkay", countMappingDataFileWorker);

            final Long countMappingDataSQLWorker = session.createQuery("select count(*) from MappingDataSQLWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();

            context.put("countMappingDataSQLWorkerOkay", countMappingDataSQLWorker);

            final Long countMartDataWorker = session.createQuery("select count(*) from MartDataWorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.OKAY)
                    .getSingleResult();

            context.put("countMartDataWorkerOkay", countMartDataWorker);

            final Long countObjects = session.createQuery("select count(*) from DVObject", Long.class).getSingleResult();
            context.put("countObjects", countObjects);

            final Long countWorkerError = session.createQuery("select count(*) from WorkerCargo m where m.status = :status", Long.class)
                    .setParameter("status", WorkerCargo.Status.ERROR)
                    .getSingleResult();
            context.put("countWorkerError", countWorkerError);
        }

        return context;
    }
}
