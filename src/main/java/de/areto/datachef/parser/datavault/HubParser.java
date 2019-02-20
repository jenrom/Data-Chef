package de.areto.datachef.parser.datavault;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import de.areto.datachef.parser.mapping.MappingParser;
import lombok.NonNull;
import org.hibernate.Session;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HubParser {

    private final Mapping mapping;
    private final Session session;

    private final Multimap<String, String> hubRoleMap = HashMultimap.create();

    public HubParser(@NonNull Mapping mapping, @NonNull Session session) {
        this.mapping = mapping;
        this.session = session;
    }

    public Mapping parseHubs() {
        mapping.getMappingColumns().stream()
                .filter(MappingColumn::hasObjectName)
                .filter(MappingColumn::isKeyColumn)
                .collect(Collectors.groupingBy(MappingColumn::getObjectName))
                .forEach(this::parseHub);

        return mapping;
    }

    private void parseHub(String hubName, List<MappingColumn> columns) {
        final Optional<Hub> persistentHub = session.byNaturalId(Hub.class)
                .using(Hub.TYPE_COLUMN, DVObject.Type.HUB)
                .using(Hub.IDENTIFIER_COLUMN, hubName)
                .loadOptional();

        final Hub transientHub = createHub(hubName, columns);

        if(persistentHub.isPresent()) {
            if(mergePersistentHub(persistentHub.get(), transientHub))
                storeHub(persistentHub.get());
        } else {
            mapping.addNewObject(transientHub);
            storeHub(transientHub);
        }
    }

    private void storeHub(Hub hub) {
        mapping.addObject(hub);

        for(String role : hubRoleMap.get(hub.getName())) {
            mapping.addObjectWithRole(hub, role);
        }
    }

    private boolean mergePersistentHub(Hub persistentHub, Hub transientHub) {
        final DVObjectDiff structureComparator = new DVObjectDiff(persistentHub, transientHub);
        structureComparator.analyze();

        if(structureComparator.hasDifferences()) {
            structureComparator.getDifferences().forEach(mapping::addIssue);
            return false;
        }

        transientHub.getComments().forEach(persistentHub::addComment);

        for (DVColumn transientColumn : transientHub.getColumns()) {
            final DVColumn persistentColumn = persistentHub.getColumnByName(transientColumn.getName());
            checkNotNull(persistentColumn,"Unable to find column %s", transientColumn.getName());
            transientColumn.getComments().forEach(persistentColumn::addComment);
        }

        return true;
    }

    private Hub createHub(String name, List<MappingColumn> mappingColumns) {
        Hub hub = new Hub(name);

        final Set<MappingColumn> roleColumns = new HashSet<>();

        for (MappingColumn mappingColumn : mappingColumns) {
            checkState(mappingColumn.isKeyColumn(), "MappingColumn must be a key column");

            if(mappingColumn.hasRoleName()) {
                roleColumns.add(mappingColumn);
                continue;
            }
            if(!hub.hasAlias() && mappingColumn.hasObjectAlias()) {
                hub.setAlias(mappingColumn.getObjectAlias());
            }
            if(!hub.hasComment() && mappingColumn.hasObjectComment()) {
                hub.addComment(mappingColumn.getObjectComment());
            }

            final DVColumn dvColumn = MappingParser.createDvColumn(mappingColumn);
            dvColumn.setKeyColumn(true);

            hub.addColumn(dvColumn);
        }

        for (MappingColumn mappingColumn : roleColumns) {
            final DVColumn dvCol = hub.getColumnByName(mappingColumn.getFinalName());

            if(dvCol == null) {
                final String msg = String.format("Column '%s' maps to unknown column '%s' in role '%s'",
                        mappingColumn.getName(), mappingColumn.getFinalName(), mappingColumn.getRoleName());
                mapping.addIssue(msg);
                continue;
            }

            if(!dvCol.getDataDomain().equals(mappingColumn.getDataDomain())) {
                final String msg = String.format("Columns '%s' and '%s' must have matching Data Domains",
                        mappingColumn.getName(), mappingColumn.getFinalName());
                mapping.addIssue(msg);
                continue;
            }

            if(mappingColumn.hasComment())
                dvCol.addComment(mappingColumn.getComment());

            hubRoleMap.put(name, mappingColumn.getRoleName());
        }

        return hub;
    }
}