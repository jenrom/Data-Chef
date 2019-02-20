package de.areto.datachef.creator;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.creator.expression.dml.*;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingObjectReference;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static de.areto.datachef.creator.expression.TableExpressionFactory.truncateExpression;

public class DMLQueueCreator implements SQLExpressionQueueCreator {

    private final Queue<SQLExpression> hubQueue = new LinkedList<>();
    private final Queue<SQLExpression> linkQueue = new LinkedList<>();
    private final Queue<SQLExpression> satQueue = new LinkedList<>();

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    private final Mapping mapping;

    public DMLQueueCreator(@NonNull Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        createObjectStatements();

        final Queue<SQLExpression> finalQueue = new LinkedList<>();
        if(dwhConfig.truncateStage()) {
            finalQueue.add(truncateExpression(dwhConfig.schemaNameRaw(), mapping.getName()));
            finalQueue.add(truncateExpression(dwhConfig.schemaNameCooked(), mapping.getName()));
        }
        finalQueue.add(createImportExpression());
        finalQueue.add(createRawToCookedExpression());
        finalQueue.addAll(hubQueue);
        finalQueue.addAll(linkQueue);
        finalQueue.addAll(satQueue);
        return finalQueue;
    }

    private void createObjectStatements() throws CreatorException {
        final Set<Link> histLinkSet = new HashSet<>();
        final TemplateConfig tplConfig = ConfigCache.getOrCreate(TemplateConfig.class);

        for(MappingObjectReference reference : mapping.getMappingObjectReferences()) {
            final DVObject object = reference.getObject();

            if(object.isHub()) {
                final PopulateExpressionCreator<Hub> creator = new PopulateExpressionCreator<>(
                        mapping, object.asHub(), tplConfig.populateHubTemplate());
                if(!reference.isDefaultRole()) creator.setRole(reference.getRole());
                hubQueue.add(creator.createExpression());
            }

            if(object.isLink()) {
                final Link link = object.asLink();
                final PopulateExpressionCreator<Link> creator = new PopulateExpressionCreator<>(
                        mapping, link, tplConfig.populateLinkTemplate());
                linkQueue.add(creator.createExpression());
                if(link.isHistoricized()) histLinkSet.add(link);
            }

            if(object.isSatellite()) {
                final PopulateExpressionCreator<Satellite> creator = new PopulateExpressionCreator<>(
                        mapping, object.asSatellite(), tplConfig.populateSatelliteTemplates()
                );
                if(!reference.isDefaultRole()) creator.setRole(reference.getRole());

                final SQLExpression satelliteExpression = creator.createExpression();

                if(mapping.isFullLoad() && dvConfig.deleteDetectionOnFullLoad()) {
                    final String deleteDetection = new SatDeleteDetectionExpressionCreator(object.asSatellite(), mapping)
                            .createExpression().getSqlCode();
                    final String mergedSqlCode = String.format("%s \n %s", satelliteExpression.getSqlCode(), deleteDetection);
                    satelliteExpression.setSqlCode(mergedSqlCode);
                }

                satQueue.add(satelliteExpression);
            }
        }

        for(Link link : histLinkSet) {
            final PopulateExpressionCreator<Link> creator = new PopulateExpressionCreator<>(
                    mapping, link, tplConfig.populateHistorySatelliteTemplates());

            final SQLExpression histSatExpr = creator.createExpression();
            final String newDesc = String.format("Populate History Satellite for %s %s for Mapping '%s' #%d",
                        link.getType(), link.getName(), mapping.getName(), 1);
            histSatExpr.setDescription(newDesc);

            if(mapping.isFullLoad() && dvConfig.deleteDetectionOnFullLoad()) {
                final String deleteDetection = new HistSatDeleteDetectionExpressionCreator(link, mapping)
                        .createExpression().getSqlCode();
                final String mergedSqlCode = String.format("%s \n %s", histSatExpr.getSqlCode(), deleteDetection);
                histSatExpr.setSqlCode(mergedSqlCode);
            }

            satQueue.add(histSatExpr);
        }
    }

    private SQLExpression createImportExpression() throws CreatorException {
        final SQLExpressionCreator creator = new ImportExpressionCreator(mapping);
        return creator.createExpression();
    }

    private SQLExpression createRawToCookedExpression() throws CreatorException {
        final SQLExpressionCreator creator = new RawToCookedExpressionCreator(mapping);
        return creator.createExpression();
    }
}
