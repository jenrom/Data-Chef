package de.areto.datachef.creator.table;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.model.datavault.Link;
import de.areto.datachef.model.jdbc.DBTable;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;

public class HistorySatelliteTableCreator implements DBTableCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    private final Link link;

    public HistorySatelliteTableCreator(@NonNull Link link) {
        this.link = link;
    }

    @Override
    public DBTable createDbTable() throws CreatorException {
        if(!link.isHistoricized()) {
            final String msg = "Creation of History Satellites is only permitted for historicized Links";
            throw new CreatorException(msg);
        }

        final String tableName = dvConfig.satNamePrefix() + link.getName() + dvConfig.histSatSuffix();
        final DBTable table = new DBTable(tableName);
        try {
            table.setSchema(DWHSpox.get().getSchema(dwhConfig.schemaNameServed()));
        } catch (SQLException e) {
            throw new CreatorException(e);
        }

        table.setComment(String.format("History Satellite for %s %s", link.getType(), link.getName()));

        table.addColumn(DBColumnFactory.createKeyColumn(link));
        table.addColumn(DBColumnFactory.createLoadIdColumn());
        table.addColumn(DBColumnFactory.createLoadDateColumn());
        table.addColumn(DBColumnFactory.createLoadDateEndColumn());

        return table;
    }
}
