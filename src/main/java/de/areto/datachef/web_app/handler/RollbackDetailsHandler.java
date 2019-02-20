package de.areto.datachef.web_app.handler;

import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.worker.RollbackWorkerCargo;
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
        path = "/rollback/details/:dbId",
        template = "rollback_details.vm"
)
public class RollbackDetailsHandler extends TemplateRouteHandler {

    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final String idString = request.params("dbId");

        if(idString == null || idString.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final long dbId = Long.valueOf(idString);
            final Optional<RollbackWorkerCargo> rollbackCargoOptional = session.byId(RollbackWorkerCargo.class)
                    .loadOptional(dbId);

            if(!rollbackCargoOptional.isPresent()) {
                throw new WebException(String.format("Mapping or Mart '%d' not found", dbId));
            }

            final RollbackWorkerCargo rollbackCargo = rollbackCargoOptional.get();
            Hibernate.initialize(rollbackCargo.getErrors());
            Hibernate.initialize(rollbackCargo.getMessages());
            Hibernate.initialize(rollbackCargo.getWarnings());

            final Map<String, Object> context = new HashMap<>();
            context.put("cargo", rollbackCargo);
            return context;
        }
    }
}