package de.areto.datachef.web_app.common;

public enum ContentType {
    APPLICATION_JSON ("application/json"),
    TEXT_HTML ("text/html"),
    TEXT_PLAIN ("text/plain"),
    WILDCARD ("*/*");

    private final String text;

    ContentType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
