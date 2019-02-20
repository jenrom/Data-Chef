package de.areto.common.watcher;

public interface DirWatcherListener {
    void fire(DirWatcherEvent event);
}
