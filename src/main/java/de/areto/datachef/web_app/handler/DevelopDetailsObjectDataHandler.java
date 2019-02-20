package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.web.ObjectDataBean;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.extern.slf4j.Slf4j;
import spark.Request;
import spark.Response;

import java.util.LinkedList;
import java.util.List;

@WebRoute(
        path = "/develop/object/details/:type/id/:dbId",
        responseTransformer = JsonTransformer.class
)
@Slf4j
public class DevelopDetailsObjectDataHandler extends RouteHandler {

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

        final String sessionId = request.session().id();
        final Mapping mapping = DevelopCallbackHandler.MAPPING_CACHE.getIfPresent(sessionId);

        if(mapping == null) {
            final String msg = String.format("No simulated Mapping for '%s' available", sessionId);
            throw new WebException(msg);
        }

        long dbId = Long.valueOf(dbIdString);
        final List<Satellite> satellites = new LinkedList<>();
        DVObject target = null;

        for (DVObject object : mapping.getMappedObjects()) {
            if(object.isSatellite() && object.asSatellite().getParent().getDbId() == dbId)
                satellites.add(object.asSatellite());
            if(object.getDbId().equals(dbId))
                target = object;
        }

        if(target == null) {
            throw new WebException("Object '" + dbId + "' not found");
        }

        return new ObjectDataBean<>(target, satellites);
    }
}
