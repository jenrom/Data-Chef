package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.web.ServedEdge;
import de.areto.datachef.model.web.ServedNode;
import de.areto.datachef.model.web.ServedPayload;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import org.hibernate.query.Query;
import spark.Request;
import spark.Response;

import java.util.Arrays;
import java.util.List;

@WebRoute(
        path = "/served/data/:mappingName",
        responseTransformer = JsonTransformer.class
)
public class ServedDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String mappingName = request.params("mappingName");

        if(mappingName == null || mappingName.isEmpty())
            throw new WebException("Parameter 'mappingName' is not set");

        final ServedPayload payload = new ServedPayload();

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {

            final String qObjectCount = "select count(*) from DVObject";
            final long oCount = session.createQuery(qObjectCount, Long.class).getSingleResult();
            if(oCount == 0)
                return payload;

            String qIds = "select distinct r.object.dbId from MappingObjectReference r";

            if(mappingName.startsWith("_search_"))
                qIds = "select distinct r.object.dbId from MappingObjectReference r " +
                        "join r.object o " +
                        "where o.name like :search";
            else if(!mappingName.equals("_all"))
                qIds += " where r.mapping.name = :mappingName";

            final Query<Long> query = session.createQuery(qIds, Long.class);

            if(mappingName.startsWith("_search_")) {
                final String search = mappingName.replace("_search_", "");
                query.setParameter("search", "%" + search + "%");
            } else if(!mappingName.equals("_all")) {
                query.setParameter("mappingName", mappingName);
            }

            final List<Long> allowedObjectIds = query.getResultList();

            final String qNodes = "select new " + ServedNode.class.getName() + "(o) " +
                    "from DVObject o " +
                    "where o.type in :types " +
                    "and o.dbId in :ids";

            final List<ServedNode> nodes = session.createQuery(qNodes, ServedNode.class)
                    .setParameter("types", Arrays.asList(DVObject.Type.HUB, DVObject.Type.LNK))
                    .setParameter("ids", allowedObjectIds)
                    .getResultList();

            final String qEdges = "select new " + ServedEdge.class.getName() + "(l.hub.dbId, l.parentLink.dbId) " +
                    "from Leg l " +
                    "join l.hub join l.parentLink " +
                    "where l.hub.dbId in :ids or l.parentLink.dbId in :ids";

            final List<ServedEdge> edges = session.createQuery(qEdges, ServedEdge.class)
                    .setParameter("ids", allowedObjectIds)
                    .getResultList();

            payload.setNodes(nodes);
            payload.setEdges(edges);

        }

        return payload;
    }
}
