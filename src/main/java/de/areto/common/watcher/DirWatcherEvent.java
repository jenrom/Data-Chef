package de.areto.common.watcher;

import lombok.Getter;
import lombok.Setter;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

@Getter
@Setter
public class DirWatcherEvent {

    public enum Type {
        CREATE,
        DELETE,
        MODIFY
    }

    private final UUID eventId = UUID.randomUUID();
    private final Type type;
    private final Path path;

    public DirWatcherEvent(Type type, Path path) {
        this.type = type;
        this.path = path;
    }

    @Override
    public String toString() {
        return "DirWatcherEvent{" +
                "type=" + type +
                ", path=" + path +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DirWatcherEvent that = (DirWatcherEvent) o;
        return Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }
}