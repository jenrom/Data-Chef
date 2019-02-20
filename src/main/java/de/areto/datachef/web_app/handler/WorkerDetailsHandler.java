package de.areto.datachef.web_app.handler;

import de.areto.common.util.StringUtility;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.compilation.SQLExpressionExecution;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@WebRoute(
        path = "/worker/details/:dbId",
        template = "worker_details.vm"
)
public class WorkerDetailsHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final String idString = request.params("dbId");

        if(idString == null || idString.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final long dbId = Long.valueOf(idString);
            final Optional<WorkerCargo> cargo = session.byId(WorkerCargo.class).loadOptional(dbId);

            if(!cargo.isPresent()) {
                throw new WebException(String.format("WorkerCargo for '%d' not found", dbId));
            }

            Hibernate.initialize(cargo.get().getMessages());
            Hibernate.initialize(cargo.get().getErrors());
            Hibernate.initialize(cargo.get().getWarnings());
            Hibernate.initialize(cargo.get().getExecutions());

            for(SQLExpressionExecution e : cargo.get().getExecutions())
                Hibernate.initialize(e.getExpression());

            final Map<String, Object> context = new HashMap<>();
            context.put("cargo", cargo.get());
            context.put("StringUtility", StringUtility.class);
            return context;
        }
    }
}
