package de.areto.common.watcher;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.*;

@Slf4j
public class DirWatcher {

    private final List<DirWatcherListener> events = new ArrayList<>();
    private final Map<WatchKey, Path> keys = new HashMap<>();

    private final WatchService watcher;
    private final Path path;
    private final boolean recursive;

    public static DirWatcher create(@NonNull Path path) throws IOException {
        return create(path, true);
    }

    public static DirWatcher create(@NonNull Path dir, boolean recursive) throws IOException {
        final WatchService watcher = FileSystems.getDefault().newWatchService();
        return new DirWatcher(dir, recursive, watcher);
    }

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    private DirWatcher(@NonNull Path dir, boolean recursive, @NonNull WatchService watcher) throws IOException {
        this.watcher = watcher;
        this.recursive = recursive;
        this.path = dir;
        if (recursive) registerAll(dir);
        else register(dir);
    }

    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        keys.put(key, dir);
    }

    private void registerAll(final Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void registerListener(DirWatcherListener w) {
        this.events.add(w);
    }

    public void processEvents() {
        log.info("Watching '{}'", path.toAbsolutePath());

        for (; ; ) { // wait for key to be signalled
            final WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            final Path dir = keys.get(key);
            if (dir == null) {
                log.error("WatchKey not recognized");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                final WatchEvent.Kind kind = event.kind();

                // ToDo - provide example of how OVERFLOW event is handled
                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                final WatchEvent<Path> ev = cast(event);
                final Path name = ev.context();
                final Path child = dir.resolve(name);

                if (log.isTraceEnabled()) log.trace("{}: {}", event.kind().name(), child);

                final DirWatcherEvent.Type type;
                if (kind == ENTRY_CREATE) {
                    type = DirWatcherEvent.Type.CREATE;
                } else if (kind == ENTRY_DELETE) {
                    type = DirWatcherEvent.Type.DELETE;
                } else { //kind == ENTRY_MODIFY
                    type = DirWatcherEvent.Type.MODIFY;
                }

                for (DirWatcherListener w : events)
                    w.fire(new DirWatcherEvent(type, child));

                // if directory is created, and watching recursively, then
                // register it and its sub-directories
                if (recursive && (kind == ENTRY_CREATE)) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException x) {
                        // ignore
                    }
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }
}
