package de.areto.datachef.model.jdbc;

import lombok.Data;

@Data
public class DBColumn {

    private String name;
    private String typeName;
    private int precision;
    private int scale;
    private boolean nullable;
    private String comment;
    private boolean identity;

    public String toSqlTypeString() {
        final StringBuilder b = new StringBuilder();
        b.append(typeName);
        if(precision != 0) {
            b.append("(").append(precision);
            if(scale != 0) b.append(",").append(scale);
            b.append(")");
        }
        return b.toString();
    }

    public boolean hasComment() {
        return comment != null && !comment.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("DBColumn{%s [%s]}", name, toSqlTypeString());
    }
}
