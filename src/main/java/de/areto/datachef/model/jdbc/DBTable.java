package de.areto.datachef.model.jdbc;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class DBTable implements Comparable<DBTable> {

    private List<DBColumn> columns = new ArrayList<>();

    private String name;
    private DBObjectType type;
    private String schema;
    private String comment;

    public DBTable() {
        type = DBObjectType.TABLE;
    }

    public DBTable(String name) {
        this();
        this.name = name;
    }

    public void addColumn(DBColumn column) {
        this.columns.add(column);
    }

    public boolean hasComment() {
        return this.comment != null && !this.comment.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBTable dbTable = (DBTable) o;
        return Objects.equals(name, dbTable.name) &&
                Objects.equals(schema, dbTable.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema);
    }

    @Override
    public String toString() {
        return "DBTable{" + "name=" + name + ", schema=" + schema + '}';
    }

    @Override
    public int compareTo(DBTable o) {
        return this.name.compareTo(o.getName());
    }
}
