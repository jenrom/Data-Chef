package de.areto.datachef.web_app.handler;

import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import javax.persistence.EntityGraph;
import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/mapping/details/:mappingName",
        template = "mapping_details.vm"
)
public class MappingDetailsHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final String mappingName = request.params("mappingName");

        if(mappingName == null || mappingName.isEmpty())
            throw new WebException("Please provide parameter 'dbId'");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final EntityGraph<?> graphMappingFull = session.getEntityGraph("graph-mapping-full");
            final String qMappingByName = "from Mapping m where m.name = :name";
            final Mapping mapping = session.createQuery(qMappingByName, Mapping.class)
                    .setParameter("name", mappingName)
                    .setHint("javax.persistence.loadgraph", graphMappingFull)
                    .getSingleResult();

            if(mapping == null)
                throw new WebException("Mapping '" + mappingName + "' not found");

            final Map<String, Object> context = new HashMap<>();
            context.put("mapping", mapping);
            context.put("active", "mapping_details");
            return context;
        }
    }
}
