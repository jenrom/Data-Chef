package de.areto.datachef.creator.table;

import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpoxUtility;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartColumn;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class MartTableCreator implements DBTableCreator {

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
    private final Mart mart;

    public MartTableCreator(@NonNull Mart mart) {
        this.mart = mart;
    }

    @Override
    public DBTable createDbTable() throws CreatorException {
        checkState(mart.isValid(), "Mart has to be valid");
        final DBTable table = new DBTable(mart.getName());

        try {
            table.setSchema(DWHSpox.get().getSchema(dwhConfig.schemaNameDiner()));
        } catch (SQLException e) {
            throw new CreatorException(e);
        }

        final String tableComment = String.format("Mart '%s'", mart.getName());
        table.setComment(DbSpoxUtility.escapeSingleQuotes(tableComment));

        final List<MartColumn> columnsSorted = mart.getColumns().stream()
                .sorted(Comparator.comparing(MartColumn::getOrderNumber))
                .collect(Collectors.toList());

        for (MartColumn martColumn : columnsSorted) {
            table.addColumn(DBColumnFactory.from(martColumn));
        }

        return table;
    }
}