package de.areto.datachef.creator.table;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.model.datavault.DVColumn;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Leg;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.jdbc.DBColumn;
import de.areto.datachef.model.jdbc.DBTable;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;

public class DVObjectTableCreator implements DBTableCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DVObject object;

    public DVObjectTableCreator(DVObject object) {
        this.object = object;
    }

    @Override
    public DBTable createDbTable() throws CreatorException {
        final String tableName = String.format("%s%s", object.getNamePrefix(), object.getName());
        final DBTable table = new DBTable(tableName);
        try {
            table.setSchema(DWHSpox.get().getSchema(dwhConfig.schemaNameServed()));
        } catch (SQLException e) {
            throw new CreatorException(e);
        }

        final String comment = String.format("DVObject %s %s", object.getType(), object.getName());
        table.setComment(comment);

        table.addColumn(DBColumnFactory.createKeyColumn(object));

        if (object.isLink()) {
            final Link link = object.asLink();
            for (Leg leg : link.getLegs()) {
                final DBColumn legColumn = DBColumnFactory.createFromLeg(leg);
                /* Self references contain only the same Hub. In order to prevent naming conflicts
                 * the leg's role is appended to the column name if it's not the default role.
                 */
                if (link.isSelfReference() && !leg.getRole().equals(Leg.DEFAULT_ROLE)) {
                    final String nameWithRole = String.format("%s_%s", legColumn.getName(), leg.getRole());
                    legColumn.setName(nameWithRole);
                }
                table.addColumn(legColumn);
            }
        }

        table.addColumn(DBColumnFactory.createLoadIdColumn());

        if (object.isSatellite()) {
            table.addColumn(DBColumnFactory.createDiffHashColumn(object.asSatellite()));
            table.addColumn(DBColumnFactory.createLoadDateColumn());
            table.addColumn(DBColumnFactory.createLoadDateEndColumn());
        }

        for (DVColumn dvColumn : object.getColumnsSorted()) {
            table.addColumn(DBColumnFactory.createFromDVColumn(dvColumn));
        }

        return table;
    }
}
