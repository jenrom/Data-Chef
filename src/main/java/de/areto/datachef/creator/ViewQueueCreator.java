package de.areto.datachef.creator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.creator.expression.view.HubViewExpressionQueueCreator;
import de.areto.datachef.creator.expression.view.LinkViewExpressionQueueCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import lombok.NonNull;
import org.hibernate.Session;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * DDLQueueCreator has the following responsibilities:
 * <ul>
 * <li>Create VIEW statements for Hubs and Links</li>
 * </ul>
 */
public class ViewQueueCreator implements SQLExpressionQueueCreator {

    private final Collection<DVObject> objects;
    private final Session session;

    private final Multimap<String, Satellite> hubSatMap = HashMultimap.create();

    public ViewQueueCreator(@NonNull Mapping mapping, @NonNull Session session) {
        this(mapping.getMappedObjects(), session);
    }

    public ViewQueueCreator(@NonNull Collection<DVObject> objects, @NonNull Session session) {
        this.objects = objects;
        this.session = session;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        final Queue<SQLExpression> queue = new LinkedList<>();
        queue.addAll(createHubViewExpressions());
        queue.addAll(createLinkViewExpressions());
        return queue;
    }

    private Queue<SQLExpression> createHubViewExpressions() throws CreatorException {
        final Queue<SQLExpression> hubQueue = new LinkedList<>();
        for (DVObject object : objects) {
            if (!object.isHub())
                continue;

            final Hub hub = object.asHub();
            final Set<Satellite> satellites = getReferencedSatellites(hub);

            hubSatMap.putAll(hub.getName(), satellites);

            hubQueue.addAll(new HubViewExpressionQueueCreator(hub, satellites).createExpressionQueue());
        }
        return hubQueue;
    }

    private Queue<SQLExpression> createLinkViewExpressions() throws CreatorException {
        final Queue<SQLExpression> linkQueue = new LinkedList<>();
        for (DVObject object : objects) {
            if (!object.isLink()) continue;
            final Link link = object.asLink();

            final Set<Satellite> lnkSats = getReferencedSatellites(link);

            linkQueue.addAll(new LinkViewExpressionQueueCreator(link, lnkSats, hubSatMap).createExpressionQueue());
        }
        return linkQueue;
    }

    private Set<Satellite> getReferencedSatellites(@NonNull DVObject object) {
        checkState(!object.isSatellite(), "Lookup object must be a Link or a Hub");

        final String qSatPerParent = "from Satellite s where s.parent = :parent";
        final List<Satellite> persistentSats = session.createQuery(qSatPerParent, Satellite.class)
                .setParameter("parent", object)
                .getResultList();

        final Set<Satellite> refSats = new HashSet<>(persistentSats);

        objects.stream().filter(DVObject::isSatellite).map(DVObject::asSatellite)
                .filter(s -> s.getParent().equals(object)).forEach(refSats::add);

        return refSats;
    }

}
