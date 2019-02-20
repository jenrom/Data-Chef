package de.areto.datachef.model.jdbc;

public enum DBObjectType {
    TABLE,
    VIEW,
    UNKNOWN;

    public static DBObjectType matchFromString(String string) {
        if(string.toLowerCase().contains("tabl"))
            return TABLE;
        else if(string.toLowerCase().contains("view"))
            return VIEW;
        else
            return UNKNOWN;
    }
}
