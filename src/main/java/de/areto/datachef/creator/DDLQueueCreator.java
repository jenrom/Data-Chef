package de.areto.datachef.creator;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.creator.expression.SQLExpressionQueueCreator;
import de.areto.datachef.creator.expression.ddl.CreateTableExpressionCreator;
import de.areto.datachef.creator.expression.ddl.ForeignKeyExpressionQueueCreator;
import de.areto.datachef.creator.expression.ddl.HistSatConstraintExpressionsCreator;
import de.areto.datachef.creator.expression.ddl.PrimaryKeyExpressionCreator;
import de.areto.datachef.creator.table.CookedTableGenerator;
import de.areto.datachef.creator.table.DVObjectTableCreator;
import de.areto.datachef.creator.table.HistorySatelliteTableCreator;
import de.areto.datachef.creator.table.RawTableGenerator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mapping.Mapping;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.LinkedList;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;

/**
 * DDLQueueCreator has the following responsibilities:
 * <ul>
 * <li>CREATE TABLE statements for RAW + COOKED table</li>
 * <li>CREATE TABLE statements for all new Data Vault objects ({@link DVObject})</li>
 * <li>If enabled: Primary Key constraints for all Data Vault tables</li>
 * <li>If enabled: Foreign Key constraints for all Data Vault tables</li>
 * <li>History tracking Satellites for historicized Links ({@link Link#isHistoricized()})</li>
 * </ul>
 */
public class  DDLQueueCreator implements SQLExpressionQueueCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);

    private final Mapping mapping;

    public DDLQueueCreator(@NonNull Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public Queue<SQLExpression> createExpressionQueue() throws CreatorException {
        final Queue<SQLExpression> queue = new LinkedList<>();
        queue.add(ddlRawTable());
        queue.add(ddlCookedTable());

        final Queue<SQLExpression> pkQueue = new LinkedList<>();
        final Queue<SQLExpression> fkQueue = new LinkedList<>();
        final Queue<SQLExpression> histSatQueue = new LinkedList<>();


        for (DVObject object : mapping.getNewObjects()) {
            queue.add(ddlObjectTable(object));

            if (object.isLink() && object.asLink().isHistoricized()) {
                final Link link = object.asLink();
                queue.add(ddlHistorySatelliteTable(link));

                final HistSatConstraintExpressionsCreator creator = new HistSatConstraintExpressionsCreator(link);
                histSatQueue.addAll(creator.createExpressionQueue());
            }

            if (dwhConfig.generatePrimaryKeys())
                pkQueue.add(ddlPrimaryKeyConstraint(object));

            if (dwhConfig.generateForeignKeys() && !object.isHub())
                fkQueue.addAll(ddlForeignKeyConstraints(object));
        }

        queue.addAll(pkQueue);
        queue.addAll(fkQueue);
        queue.addAll(histSatQueue);

        return queue;
    }

    private SQLExpression ddlRawTable() throws CreatorException {
        final RawTableGenerator rawTableGenerator = new RawTableGenerator(mapping);
        final DBTable rawTable = rawTableGenerator.createDbTable();
        final SQLExpressionCreator expressionCreator = new CreateTableExpressionCreator(rawTable);
        return expressionCreator.createExpression();
    }

    private SQLExpression ddlCookedTable() throws CreatorException {
        final CookedTableGenerator cookedTableGenerator = new CookedTableGenerator(mapping);
        final DBTable cookedTable = cookedTableGenerator.createDbTable();
        final SQLExpressionCreator expressionCreator = new CreateTableExpressionCreator(cookedTable);
        return expressionCreator.createExpression();
    }

    private SQLExpression ddlObjectTable(@NonNull DVObject object) throws CreatorException {
        final DVObjectTableCreator tableCreator = new DVObjectTableCreator(object);
        final DBTable objectTable = tableCreator.createDbTable();
        final SQLExpressionCreator expCreator = new CreateTableExpressionCreator(objectTable);
        return expCreator.createExpression();
    }

    private SQLExpression ddlHistorySatelliteTable(@NonNull Link link) throws CreatorException {
        final HistorySatelliteTableCreator tableCreator = new HistorySatelliteTableCreator(link);
        final DBTable histSatTable = tableCreator.createDbTable();
        final SQLExpressionCreator expCreator = new CreateTableExpressionCreator(histSatTable);
        return expCreator.createExpression();
    }

    private SQLExpression ddlPrimaryKeyConstraint(@NonNull DVObject object) throws CreatorException {
        checkState(dwhConfig.generatePrimaryKeys(), "PK generation must be enabled");
        final SQLExpressionCreator creator = new PrimaryKeyExpressionCreator(object);
        return creator.createExpression();
    }

    private Queue<SQLExpression> ddlForeignKeyConstraints(@NonNull DVObject object) throws CreatorException {
        checkState(dwhConfig.generateForeignKeys(), "PK generation must be enabled");
        checkState(!object.isHub(), "FK generation only allowed for Links and Satellites");
        final SQLExpressionQueueCreator creator = new ForeignKeyExpressionQueueCreator(object);
        return creator.createExpressionQueue();
    }
}