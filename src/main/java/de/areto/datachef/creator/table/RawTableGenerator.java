package de.areto.datachef.creator.table;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpoxUtility;
import de.areto.datachef.model.jdbc.DBColumn;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;

public class RawTableGenerator implements DBTableCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final Mapping mapping;

    public RawTableGenerator(Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public DBTable createDbTable() throws CreatorException {
        final DBTable table = new DBTable(mapping.getName());
        try {
            table.setSchema(DWHSpox.get().getSchema(dwhConfig.schemaNameRaw()));
        } catch (SQLException e) {
            throw new CreatorException(e);
        }

        final String tableComment = String.format("Staging table for Mapping '%s'", mapping.getName());
        table.setComment(DbSpoxUtility.escapeSingleQuotes(tableComment));

        /* Include all columns that are imported (including ignored columns). Columns that define
         * a calculation ('clc') get their value on the way from RAW to COOKED and are therefore
         * skipped in the RAW table. Also, all columns in the RAW table are nullable.
         */
        for (MappingColumn mappingColumn : mapping.getMappingColumnsOriginalOrder()) {
            if(mappingColumn.hasCalculation()) continue;

            final DBColumn dbColumn = DBColumnFactory.create(mappingColumn.getName(), mappingColumn.getComment(),
                    true, mappingColumn.getDataDomain());
            table.addColumn(dbColumn);
        }

        return table;
    }
}
