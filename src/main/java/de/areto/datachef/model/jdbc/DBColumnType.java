package de.areto.datachef.model.jdbc;

import lombok.Data;

@Data
public class DBColumnType {

    private int type;
    private String typeName;
    private String createParams;
    private boolean nullable;
    private boolean caseSensitive;
    private int minimumScale;
    private int maximumScale;
    private int precision;

    @Override
    public String toString() {
        return "DBColumnType{" +
                "typeName='" + typeName + '\'' +
                '}';
    }
}
