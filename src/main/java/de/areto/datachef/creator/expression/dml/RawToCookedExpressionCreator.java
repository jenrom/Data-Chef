package de.areto.datachef.creator.expression.dml;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.Leg;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingObjectReference;
import de.areto.datachef.model.template.HashColumn;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class RawToCookedExpressionCreator implements SQLExpressionCreator {

    private static final String TEMPLATE = "staging/raw_to_cooked.vm";
    private final Mapping mapping;
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    public RawToCookedExpressionCreator(@NonNull Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public SQLExpression createExpression() throws CreatorException {
        final Map<String, Object> context = new HashMap<>();
        context.put("mapping", mapping);
        context.put("hashColumns", createHashColumns());

        final SQLExpression expression = new SQLExpression();
        expression.setDescription(String.format("RAW to COOKED for Mapping '%s'", mapping.getName()));
        expression.setQueryType(SQLExpression.QueryType.RAW_2_COOKED);
        try {
            expression.setSqlCode(new SQLTemplateRenderer(TEMPLATE).render(context));
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }

        return expression;
    }

    private List<HashColumn> createHashColumns() {
        final List<HashColumn> hashColumns = new ArrayList<>();

        for (MappingObjectReference ref : mapping.getMappingObjectReferencesSorted()) {
            if (ref.getObject().isHub()) {
                final HashColumn rcColumn = getHubHashRawCookedColumn(ref);
                hashColumns.add(rcColumn);
            } else if(ref.getObject().isLink()) {
                final HashColumn rcColumn = getLinkHashRawCookedColumn(ref);
                hashColumns.add(rcColumn);
            } else {
                final HashColumn rcColumn = getDiffHashRawCookedColumn(ref);
                hashColumns.add(rcColumn);
            }
        }

        return hashColumns;
    }

    private HashColumn getDiffHashRawCookedColumn(MappingObjectReference ref) {
        checkState(ref.getObject().isSatellite(), "Referenced Object must be a Satellite");

        String columnName = ref.getObject().getName() + dvConfig.satDiffKeySuffix();
        columnName = ref.isDefaultRole() ? columnName : columnName + "_" + ref.getRole();
        final HashColumn rcColumn = new HashColumn(columnName);

        for(DVColumn col : ref.getObject().getColumnsSorted()) {
            final String hashColName = String.format("%s%s_%s", ref.getObject().getNamePrefix(),
                    ref.getObject().getName(),
                    ref.isDefaultRole() ? col.getName() : col.getName() + "_" + ref.getRole());
            rcColumn.getHashColumns().add(hashColName);
        }
        return rcColumn;
    }

    private HashColumn getLinkHashRawCookedColumn(MappingObjectReference ref) {
        checkState(ref.getObject().isLink(), "Referenced Object must be a Link");

        final String name = ref.getObject().getName() + ref.getObject().getKeySuffix();
        final HashColumn rcColumn = new HashColumn(name);

        for(Leg leg : ((Link) ref.getObject()).getLegsSorted()) {
            for(DVColumn dvCol : leg.getHub().getColumnsSorted()) {
                final String hashColName = String.format("%s%s_%s", leg.getHub().getNamePrefix(),
                        leg.getHub().getName(), leg.isDefaultRole() ? dvCol.getName()
                        : dvCol.getName() + "_" + leg.getRole());
                rcColumn.getHashColumns().add(hashColName);
            }
        }

        return rcColumn;
    }

    private HashColumn getHubHashRawCookedColumn(MappingObjectReference ref) {
        checkState(ref.getObject().isHub(), "Referenced Object must be a Hub");

        String name = ref.getObject().getName() + ref.getObject().getKeySuffix();
        if (!ref.isDefaultRole()) name += "_" + ref.getRole();

        final HashColumn rcColumn = new HashColumn(name);

        for (DVColumn col : ref.getObject().getColumnsSorted()) {
            final String hashColumnName = String.format("%s%s_%s", ref.getObject().getNamePrefix(),
                    ref.getObject().getName(),
                    ref.isDefaultRole() ? col.getName() : col.getName() + "_" + ref.getRole());
            rcColumn.getHashColumns().add(hashColumnName);
        }

        return rcColumn;
    }
}