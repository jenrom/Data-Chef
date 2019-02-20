package de.areto.datachef.model.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class PrimaryKey {

    private final String schemaName;
    private final String tableName;
    private final String name;
    private final List<String> columns = new ArrayList<>();

    public void addColumn(String column) {
        columns.add(column);
    }
}
