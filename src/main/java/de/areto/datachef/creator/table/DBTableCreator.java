package de.areto.datachef.creator.table;

import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.jdbc.DBTable;

public interface DBTableCreator {

    DBTable createDbTable() throws CreatorException;
}
