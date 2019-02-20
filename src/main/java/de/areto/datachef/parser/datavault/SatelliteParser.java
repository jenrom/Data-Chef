package de.areto.datachef.parser.datavault;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.areto.datachef.exceptions.ParseException;
import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import de.areto.datachef.parser.mapping.MappingParser;
import org.hibernate.Session;

import java.util.*;

public class SatelliteParser {

    private final Multimap<String, MappingColumn> columnMap = HashMultimap.create();
    private final HashMap<String, DVObject> parentMap = new HashMap<>();
    private final Multimap<String, String> roleMap = HashMultimap.create();

    private final Mapping mapping;
    private final Session session;

    public SatelliteParser(Mapping mapping, Session session) {
        this.mapping = mapping;
        this.session = session;
    }

    public Mapping parseSatellites() {
        final Collection<MappingColumn> roleColumns = new HashSet<>();
        final Map<String, Satellite> parsedSatellites = new HashMap<>();

        for(MappingColumn col : mapping.getMappingColumns()) {
            if(col.isKeyColumn() || col.isIgnored())
                continue;

            if(col.hasRoleName()) {
                roleColumns.add(col);
                continue;
            }

            try {
                final String satName = getSatelliteName(col);
                parseParent(col, satName);
                columnMap.put(satName, col);
            } catch (ParseException e) {
                mapping.addIssue(e.getMessage());
            }
        }

        for(String satName : columnMap.keySet()) {
            final Collection<MappingColumn> relevantColumns = columnMap.get(satName);

            if(!parentMap.containsKey(satName)) {
                final String msg = String.format("Unable to find parent for Satellite '%s'", satName);
                mapping.addIssue(msg);
                continue;
            }

            final DVObject parent = parentMap.get(satName);
            final Satellite satellite = createSatellite(satName, parent, relevantColumns);
            parsedSatellites.put(satName, satellite);
        }

        // ToDo (later): Check if Hub in specified role exists...

        for(MappingColumn col : roleColumns) {
            try {
                final String satName = getSatelliteName(col);
                if(!parsedSatellites.containsKey(satName)) {
                    final String msg = String.format("Column '%s' - Satellite in role '%s' is not defined",
                            col.getName(), col.getRoleName());
                    mapping.addIssue(msg);
                    continue;
                }

                final Satellite satellite = parsedSatellites.get(satName);
                final DVColumn roleCol = MappingParser.createDvColumn(col);
                final DVColumn realColumn = satellite.getColumnByName(col.getFinalName());

                if(realColumn == null) {
                    final String msg = String.format("Column '%s' - Satellite column '%s' not found in Satellite '%s'",
                            col.getName(), col.getFinalName(), satName);
                    mapping.addIssue(msg);
                    continue;
                }

                final DVColumnDiff colComparator = new DVColumnDiff(realColumn, roleCol);
                if(!colComparator.analyze()) {
                    colComparator.getDifferences().forEach(mapping::addIssue);
                    continue;
                }

                roleCol.getComments().forEach(realColumn::addComment);
                roleMap.put(satName, col.getRoleName());
            } catch (ParseException e) {
                mapping.addIssue(e.getMessage());
            }
        }

        parsedSatellites.values().forEach(this::checkAndStoreSatellite);

        return mapping;
    }

    private void checkAndStoreSatellite(Satellite transientSat) {
        final Optional<Satellite> persistentSat = session.byNaturalId(Satellite.class)
                .using(Satellite.TYPE_COLUMN, DVObject.Type.SAT)
                .using(DVObject.IDENTIFIER_COLUMN, transientSat.getName())
                .loadOptional();

        if(persistentSat.isPresent()) {
            DVObjectDiff comparator = new DVObjectDiff(persistentSat.get(), transientSat);
            comparator.analyze();
            if(comparator.hasDifferences()) {
                comparator.getDifferences().forEach(mapping::addIssue);
            } else {
                storeSatellite(persistentSat.get());
            }
        } else {
            storeSatellite(transientSat);
            mapping.addNewObject(transientSat);
        }
    }

    private void storeSatellite(Satellite satellite) {
        mapping.addObject(satellite);

        for(String role : roleMap.get(satellite.getName())) {
            mapping.addObjectWithRole(satellite, role);
        }
    }

    private void parseParent(MappingColumn mCol, String satName) throws ParseException {
        if(parentMap.containsKey(satName))
            return;

        final Optional<DVObject> parent;

        if(mCol.isPartOfHubSatellite()) {
            parent = mapping.getMappedObjects().stream()
                    .filter(DVObject::isHub)
                    .filter(h -> h.getName().equals(mCol.getObjectName()))
                    .findAny();
        } else {
            parent = mapping.getMappedObjects().stream()
                    .filter(DVObject::isLink)
                    .filter(l -> l.getName().equals(mCol.getLinkSatelliteName()))
                    .findAny();
        }

        if(!parent.isPresent()) {
            final String msgTpl = "Column '%s' - Unable to find parent for Satellite '%s'";
            final String msg = String.format(msgTpl, mCol.getName(), satName);
            throw new ParseException(msg);
        } else {
            parentMap.put(satName, parent.get());
        }
    }

    private String getSatelliteName(MappingColumn mCol) throws ParseException {
        if(mCol.isPartOfHubSatellite()) {
            return mCol.hasSatelliteName() ? mCol.getSatelliteName() : mCol.getObjectName();
        } else {
            if(!mCol.hasLinkSatelliteName()) {
                final String msgTpl = "Column '%s' - Name of Link Satellite ('ls') is missing";
                final String msg = String.format(msgTpl, mCol.getName());
                throw new ParseException(msg);
            }
            // 'sn' overwrites 'ls'
            return mCol.hasSatelliteName() ? mCol.getSatelliteName() : mCol.getLinkSatelliteName();
        }
    }

    private Satellite createSatellite(String name, DVObject parent, Collection<MappingColumn> columns) {
        final Satellite satellite = new Satellite();
        satellite.setParent(parent);
        satellite.setName(name);
        columns.stream().map(MappingParser::createDvColumn).forEach(satellite::addColumn);
        return satellite;
    }
}