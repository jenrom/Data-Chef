package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Leg;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.web.ServedEdge;
import de.areto.datachef.model.web.ServedNode;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.NonNull;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@WebRoute(
        path = "/develop/details",
        template = "develop_details.vm"
)
public class DevelopDetailsHandler extends TemplateRouteHandler {
    @Override
    public Map<String, Object> createContext(Request request, Response response) throws Exception {
        final String sessionId = request.session().id();
        final Mapping mapping = DevelopCallbackHandler.MAPPING_CACHE.getIfPresent(sessionId);

        if(mapping == null) {
            final String msg = String.format("No simulated Mapping for '%s' available", sessionId);
            throw new WebException(msg);
        }

        final Map<String, Object> context = new HashMap<>();
        context.put("mapping", mapping);
        context.put("nodes", buildNodeArray(mapping));
        context.put("edges", buildEdgesArray(mapping));
        context.put("active", "develop-details");
        return context;
    }

    private String buildNodeArray(@NonNull Mapping mapping) throws Exception {
        final List<ServedNode> nodes = new LinkedList<>();
        long idConuter = 0;
        for (DVObject object : mapping.getMappedObjects()) {
            object.setDbId(idConuter);
            final ServedNode node = new ServedNode(object);
            nodes.add(node);
            idConuter++;
        }

        return new JsonTransformer().render(nodes);
    }

    private String buildEdgesArray(@NonNull Mapping mapping) throws Exception {
        final List<ServedEdge> edges = new LinkedList<>();
        for (DVObject object : mapping.getMappedObjects()) {
            if(object.isSatellite())  {
                final Satellite s = object.asSatellite();
                edges.add(new ServedEdge(s.getDbId(), s.getParent().getDbId()));
            }
            if(object.isLink()) {
                for(Leg leg : object.asLink().getLegs()) {
                    edges.add(new ServedEdge(object.getDbId(), leg.getHub().getDbId()));
                }
            }
        }
        return new JsonTransformer().render(edges);
    }
}
