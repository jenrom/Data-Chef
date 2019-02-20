package de.areto.datachef.model.template;

import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

@Data
public class ForeignKey {

    @NonNull
    private final String schemaName;
    @NonNull
    private final String tableName;
    @NonNull
    private final String name;
    @NonNull
    private final String targetSchema;
    @NonNull
    private final String targetTable;

    private final List<String> columns = new ArrayList<>();
    private final List<String> referencedColumns = new ArrayList<>();

    public void addColumnReference(String from, String to) {
        columns.add(from);
        referencedColumns.add(to);
    }

}
