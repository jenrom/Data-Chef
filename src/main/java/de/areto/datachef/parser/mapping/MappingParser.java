package de.areto.datachef.parser.mapping;

import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.DataDomain;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import de.areto.datachef.model.mapping.MappingColumnReference;
import de.areto.datachef.model.mapping.MappingObjectReference;
import de.areto.datachef.parser.antlr4.SinkDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import de.areto.datachef.parser.datavault.HubParser;
import de.areto.datachef.parser.datavault.LinkParser;
import de.areto.datachef.parser.datavault.SatelliteParser;
import lombok.NonNull;
import org.hibernate.Session;

import java.util.*;

public class MappingParser extends SinkDSLBaseVisitor<Mapping> {

    private final Mapping mapping = new Mapping();

    private final Session session;

    private String globalObjectName;
    private boolean hasGlobalObjectName;

    private String globalObjectAlias;
    private boolean hasGlobalObjectAlias;

    private String globalObjectComment;
    private boolean hasGlobalObjectComment;

    private final Set<String> columnNames = new HashSet<>();
    private Map<String, MappingColumn> mappingColumnMap = new HashMap<>();

    private SinkDSLParser.LinkRelationsContext linkRelationsContext;

    public MappingParser(@NonNull String mappingName, @NonNull Session session) {
        this.mapping.setName(mappingName);
        this.session = session;
    }

    public static DVColumn createDvColumn(@NonNull MappingColumn mappingColumn) {
        final DVColumn dvColumn = new DVColumn();
        dvColumn.setName(mappingColumn.getFinalName());
        dvColumn.setDataDomain(mappingColumn.getDataDomain());
        if(mappingColumn.hasComment()) {
            dvColumn.addComment(mappingColumn.getComment());
        }
        return dvColumn;
    }

    @Override
    public Mapping visitCompilationUnit(SinkDSLParser.CompilationUnitContext ctx) {
        new MappingConfigurationParser(mapping, session).parseConfiguration(ctx);
        visitMapping(ctx.mapping());
        return mapping;
    }

    @Override
    public Mapping visitMapping(SinkDSLParser.MappingContext ctx) {
        parseMappingParameter(ctx);

        if(ctx.mappingColumn == null || ctx.mappingColumn().isEmpty()) {
            mapping.addIssue("Mapping must define at least one column");
            return this.mapping;
        }

        for (SinkDSLParser.MappingColumnContext columnContext : ctx.mappingColumn()) {
            parseMappingColumn(columnContext);
        }

        new MappingColumnValidator(mapping).validate();

        new HubParser(mapping, session).parseHubs();
        new LinkParser(mapping, session).visitLinkRelations(linkRelationsContext);
        new SatelliteParser(mapping, session).parseSatellites();

        this.parseMappingColumnReferences();

        return mapping;
    }

    private void parseMappingColumn(SinkDSLParser.MappingColumnContext columnContext) {
        final MappingColumn column = new MappingColumnParser().visitMappingColumn(columnContext);

        if (columnNames.contains(column.getName())) {
            String msg = "Column '%s' - Duplicate column names is prohibited";
            msg = String.format(msg, column.getName());
            mapping.addIssue(msg);
        } else {
            columnNames.add(column.getName());
        }

        final Optional<DataDomain> dataDomain = session.byId(DataDomain.class)
                .loadOptional(column.getDataDomainName());
        if(!dataDomain.isPresent()) {
            final String msg = String.format("Column '%s' - Data Domain '%s' not found", column.getName(),
                    column.getDataDomainName());
            mapping.addIssue(msg);
        } else {
            column.setDataDomain(dataDomain.get());
        }

        if (!column.hasObjectName() && hasGlobalObjectName) {
            column.setObjectName(globalObjectName);
        }

        final boolean belongsToGlobalObject = hasGlobalObjectName && column.getObjectName().equals(globalObjectName);
        if(belongsToGlobalObject) {
            if(!column.hasObjectComment())
                column.setObjectComment(globalObjectComment);

            if(!column.hasObjectAlias())
                column.setObjectAlias(globalObjectAlias);
        }

        mapping.addMappingColumn(column);
        mappingColumnMap.put(column.getFinalName(), column);
    }

    private void parseMappingColumnReferences() {
        for(MappingObjectReference objectReference : mapping.getMappingObjectReferences()) {
            for(DVColumn dvColumn : objectReference.getObject().getColumns()) {
                if(!mappingColumnMap.containsKey(dvColumn.getName())) {
                    final String msg = String.format("MappingColumn for DVColumn '%s' of %s not found",
                            dvColumn.getName(), objectReference.getObject());
                    mapping.addIssue(msg);
                    continue;
                }

                final MappingColumn mappedMappingColumn = mappingColumnMap.get(dvColumn.getName());
                final MappingColumnReference columnReference = new MappingColumnReference();
                columnReference.setDvColumn(dvColumn);
                if(!objectReference.getRole().equals(MappingObjectReference.DEFAULT_ROLE)) {
                    columnReference.setRole(objectReference.getRole());
                }
                mappedMappingColumn.addMappingColumnReference(columnReference);
            }
        }
    }

    private void parseMappingParameter(SinkDSLParser.MappingContext ctx) {
        for (SinkDSLParser.MappingParamContext paramCtx : ctx.mappingParam()) {
            if(paramCtx.linkRelations() != null) {
                linkRelationsContext = paramCtx.linkRelations();
            }

            if (paramCtx.mappingKeyAndId() != null) {
                final String value = paramCtx.mappingKeyAndId().value.getText();
                if (paramCtx.mappingKeyAndId().key.getText().equals("on")) {
                    globalObjectName = value;
                    hasGlobalObjectName = true;
                }
                if (paramCtx.mappingKeyAndId().key.getText().equals("oa")) {
                    globalObjectAlias = value;
                    hasGlobalObjectAlias = true;
                }
            }
            if (paramCtx.mappingKeyAndString() != null) {
                if (paramCtx.mappingKeyAndString().key.getText().equals("ocmnt")) {
                    globalObjectComment = paramCtx.mappingKeyAndString().value.getText();
                    if (globalObjectComment.startsWith("\"") && globalObjectComment.endsWith("\"")) {
                        globalObjectComment = globalObjectComment.substring(1, globalObjectComment.length() - 1);
                    }
                    hasGlobalObjectComment = true;
                }
            }
        }

        if(hasGlobalObjectAlias && !hasGlobalObjectName) {
            final String msg = "Globally defined alias only allowed in combination with globally defined 'on'";
            mapping.addIssue(msg);
        }

        if(hasGlobalObjectComment && !hasGlobalObjectName) {
            final String msg = "Globally defined 'ocmnt' only allowed in combination with globally defined 'on'";
            mapping.addIssue(msg);
        }
    }
}