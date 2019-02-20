package de.areto.datachef.creator.table;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpoxUtility;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.jdbc.DBColumn;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import de.areto.datachef.model.mapping.MappingObjectReference;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;

import static de.areto.datachef.creator.table.DBColumnFactory.*;

public class CookedTableGenerator implements DBTableCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final Mapping mapping;

    public CookedTableGenerator(Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public DBTable createDbTable() throws CreatorException {
        final DBTable table = new DBTable(mapping.getName());

        try {
            table.setSchema(DWHSpox.get().getSchema(dwhConfig.schemaNameCooked()));
        } catch (SQLException e) {
            throw new CreatorException(e);
        }

        final String tableComment = String.format("Cooked table for Mapping '%s'", mapping.getName());
        table.setComment(DbSpoxUtility.escapeSingleQuotes(tableComment));

        for (MappingObjectReference reference : mapping.getMappingObjectReferencesSorted()) {
            /* Satellites use PK of parent (either Hub oder Link) whose PK column will be created
             * once the loop reaches the parents reference */
            if (!reference.getObject().isSatellite()) {
                final DBColumn keyColumn = createKeyColumn(reference.getObject());

                // If object is not referenced in default role, role is appended
                if (!reference.isDefaultRole()) {
                    final String colNameWithRole = String.format("%s_%s", keyColumn.getName(), reference.getRole());
                    keyColumn.setName(colNameWithRole);
                }

                table.addColumn(keyColumn);
            } else {
                final DBColumn diffHashColumn = createDiffHashColumn((Satellite) reference.getObject());

                // If object is not referenced in default role, role is appended
                if (!reference.isDefaultRole()) {
                    final String colNameWithRole = String.format("%s_%s", diffHashColumn.getName(), reference.getRole());
                    diffHashColumn.setName(colNameWithRole);
                }
                table.addColumn(diffHashColumn);
            }
        }

        table.addColumn(createLoadIdColumn());
        table.addColumn(createLoadDateColumn());

        for (MappingColumn mappingColumn : mapping.getMappingColumnsSorted()) {
            // Skip ignored columns
            if (mappingColumn.isIgnored()) continue;

            // Columns will be renamed in cooked table
            String ckdTableName = mappingColumn.getCookedTableName();

            final DBColumn dbColumn = create(ckdTableName, mappingColumn.getComment(),
                    true, mappingColumn.getDataDomain());
            table.addColumn(dbColumn);
        }

        return table;
    }
}
