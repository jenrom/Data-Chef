package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.web.ObjectDataBean;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.List;
import java.util.Optional;

@WebRoute(
        path = "/object/details/:type/id/:dbId",
        responseTransformer = JsonTransformer.class
)
@Slf4j
public class ObjectDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String dbIdString = request.params("dbId");
        final String typeString = request.params("type");

        if (dbIdString == null || dbIdString.isEmpty()) {
            throw new IllegalArgumentException("Parameter dbId not set");
        }

        if (typeString == null || typeString.isEmpty()) {
            throw new IllegalArgumentException("Parameter type not set");
        }

        final DVObject.Type type = DVObject.Type.valueOf(typeString);
        if(type != DVObject.Type.LNK && type != DVObject.Type.HUB) {
            throw new IllegalArgumentException("Type " + type + " not supported");
        }

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            long dbId = Long.valueOf(dbIdString);

            final ObjectDataBean bean;

            if (type == DVObject.Type.HUB) {
                final Hub hub = getHub(session, dbId).orElseThrow(() ->
                        new WebException("Hub with id " + dbId + " not found")
                );
                final List<Satellite> satList = getSatellites(session, hub);
                bean = new ObjectDataBean<>(hub, satList);
            } else {
                final Link link = getLink(session, dbId).orElseThrow(() ->
                        new WebException("Link with id " + dbId + " not found")
                );
                final List<Satellite> satList = getSatellites(session, link);
                bean = new ObjectDataBean<>(link, satList);
            }

            HibernateUtility.initialize(bean.getParent());
            for (Object o : bean.getSatelliteList()) {
                HibernateUtility.initialize(o);
            }

            return bean;
        }

    }

    private List<Satellite> getSatellites(@NonNull Session session, @NonNull DVObject parent) {
        final String q = "from Satellite s where s.parent = :parent";
        return session.createQuery(q, Satellite.class)
                .setParameter("parent", parent)
                .getResultList();
    }

    private Optional<Hub> getHub(@NonNull Session session, long id) {
        return session.byId(Hub.class).loadOptional(id);
    }

    private Optional<Link> getLink(@NonNull Session session, long id) {
        return session.byId(Link.class).loadOptional(id);
    }
}
