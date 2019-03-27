package de.areto.datachef.parser.datavault;

import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Leg;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingObjectReference;
import de.areto.datachef.parser.antlr4.SinkDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class LinkParser extends SinkDSLBaseVisitor<Mapping> {

    private final Mapping mapping;
    private final Session session;
    private final Set<Hub> hubs;

    // Maps Hubs to their names and aliases
    private final Map<String, Hub> hubNameAliasMap = new HashMap<>();
    private final Map<String, String> roleMap = new HashMap<>();


    public LinkParser(Mapping mapping, Session session) {
        this.mapping = mapping;
        this.session = session;

        this.hubs = mapping.getMappedObjects().stream()
                .filter(DVObject::isHub)
                .map(o -> (Hub) o).collect(Collectors.toSet());

        fillHubNameAliasRoleMap();
    }

    @Override
    public Mapping visitLinkRelations(SinkDSLParser.LinkRelationsContext ctx) {
        if (ctx == null) {
            if(hubs.size() > 1) {
                final Link globalLink = createGlobalMappingLink();
                checkAndStoreLink(globalLink);
            }
        } else {
            ctx.relation().stream().map(this::parseLinkFromRelation).forEach(this::checkAndStoreLink);
        }
        return mapping;
    }

    private void checkAndStoreLink(Link transientLink) {
        final Optional<Link> persistentLink = session.byNaturalId(Link.class)
                .using(Link.TYPE_COLUMN, DVObject.Type.LNK)
                .using(Link.IDENTIFIER_COLUMN, transientLink.getName())
                .loadOptional();

        if (persistentLink.isPresent()) {
            final DVObjectDiff comp = new DVObjectDiff(persistentLink.get(), transientLink);
            comp.analyze();

            if (comp.hasDifferences()) {
                comp.getDifferences().forEach(mapping::addIssue);
            } else {
                mapping.addObject(persistentLink.get());
            }
        } else {
            mapping.addObject(transientLink);
            mapping.addNewObject(transientLink);
        }
    }

    private void fillHubNameAliasRoleMap() {
        for (Hub h : hubs) {
            if (h.hasAlias()) {
                hubNameAliasMap.put(h.getAlias(), h);
            }
            hubNameAliasMap.put(h.getName(), h);
        }

        mapping.getMappingObjectReferences().stream()
                .filter(r -> r.getObject().isHub())
                .filter(r -> !r.getRole().equals(MappingObjectReference.DEFAULT_ROLE))
                .forEach(r -> roleMap.put(r.getRole(), r.getObject().getName()));
    }

    private Link createGlobalMappingLink() {
        checkState(hubs.size() > 1, "There must be more than one Hub present");
        final String linkName = Link.buildLinkName(hubs);
        Link link = new Link(linkName);
        for (Hub h : hubs) {
            link.addLeg(new Leg(h, true));
        }
        return link;
    }

    private Link parseLinkFromRelation(@NonNull SinkDSLParser.RelationContext relationContext) {
        final String customLinkName = relationContext.name.getText();
        final Link link = new Link(customLinkName);

        final boolean historicized = relationContext.historized != null;
        link.setHistoricized(historicized);

        boolean dkSyntax = false;

        final Set<String> hubNameSet = new HashSet<>();
        final Stack<String> hubNameStack = new Stack<>();
        final Set<String> hubReferenceSet = new HashSet<>();

        for (SinkDSLParser.ReferenceContext referenceContext : relationContext.reference()) {
            final boolean driving = referenceContext.driving != null;
            if (driving && !dkSyntax) dkSyntax = true;
            final String hubReference = referenceContext.hub.getText();
            final boolean roleReference = roleMap.containsKey(hubReference);
            final String lookup = roleReference ? roleMap.get(hubReference) : hubReference;

            if (!roleReference && !hubNameAliasMap.containsKey(lookup)) {
                final String msg = String.format("Link references unknown Hub '%s'", hubReference);
                mapping.addIssue(msg);
                continue;
            }

            final Hub refHub = hubNameAliasMap.get(lookup);
            final String hubName = refHub.getName();
            hubNameSet.add(hubName);
            hubNameStack.push(hubName);
            hubReferenceSet.add(hubReference);

            final int refCount = Collections.frequency(hubNameStack, hubName);

            // Self reference?
            if(refCount >= 2) {
                if(hubNameSet.contains(hubName) && hubNameSet.size() == 1) {
                	log.debug("True self-reference for link {} detected", link.getName());
                    link.setSelfReference(true);
                } else if (hubReferenceSet.size() < hubNameStack.size()) {
                	// hubReferenceSet < hubNameStack means we have real duplications
                	final String mTpl = "Link '%s' invalid: at least %d references to the same Hub '%s'";
                    final String msg = String.format(mTpl, link.getName(), refCount, hubName);
                    mapping.addIssue(msg);
                    break;
                } else {
                	log.debug("Fake self-reference for link {} detected (multi-reference case)", link.getName());
                	link.setSelfReference(true);
                }
            }

            final Leg leg = new Leg(refHub, driving);
            if (roleReference) leg.setRole(hubReference);
            link.addLeg(leg);
        }

        if(!dkSyntax) {
            final String mTpl = "Link '%s' invalid: no Driving Key defined";
            final String msg = String.format(mTpl, link.getName());
            mapping.addIssue(msg);
        }

        return link;
    }
}