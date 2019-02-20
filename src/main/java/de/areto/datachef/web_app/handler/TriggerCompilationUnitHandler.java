package de.areto.datachef.web_app.handler;

import de.areto.datachef.Application;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.compilation.CompilationUnit;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.Optional;

@WebRoute(
        path = "/trigger/:unitName"
)
public class TriggerCompilationUnitHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String unitName = request.params("unitName");

        if(unitName == null || unitName.isEmpty()) {
            throw new WebException("Parameter 'unitName' is not set");
        }

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final Optional<CompilationUnit> unit = session.byNaturalId(CompilationUnit.class)
                    .using("name", unitName).loadOptional();

            if(!unit.isPresent()) {
                final String msg = String.format("Compilation Unit '%s' not found", unitName);
                throw new WebException(msg);
            }

            if(unit.get() instanceof Mapping) {
                final Mapping mapping = (Mapping) unit.get();

                if(mapping.getStagingMode().equals(StagingMode.FILE)) {
                    final String msg = String.format("Mapping '%s' cannot only be triggered by files in Sink", unitName);
                    throw new WebException(msg);
                }
            }

            Application.get().getWorkerService().executeTrigger(unitName);

            return "";
        }


    }
}
