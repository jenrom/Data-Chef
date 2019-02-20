package de.areto.datachef.model.sink;

public interface SinkEvent {

    void fire(Type t, SinkFile file);

    enum Type {
        REGISTERED_MAPPING,
        REGISTERED_DATA_FILE
    }
}
