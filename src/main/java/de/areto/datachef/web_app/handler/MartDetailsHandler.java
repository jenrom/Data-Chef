package de.areto.datachef.web_app.handler;

import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.persistence.HibernateInitializer;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@WebRoute(
        path = "/mart/details/:martName",
        template = "mart_details.vm"
)
public class MartDetailsHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final String martName = request.params("martName");

        if(martName == null || martName.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final Optional<Mart> martOptional = session.byNaturalId(Mart.class)
                    .using("name", martName)
                    .loadOptional();

            if(!martOptional.isPresent())
                throw new WebException("Mart '" + martName + "' not found");

            final Mart mart = martOptional.get();
            HibernateInitializer.initialize(mart);

            final Map<String, Object> context = new HashMap<>();
            context.put("mart", mart);
            context.put("active", "mart_details");
            return context;
        }
    }
}
