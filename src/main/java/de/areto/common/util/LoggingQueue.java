package de.areto.common.util;

import com.google.common.collect.EvictingQueue;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Queue;

public class LoggingQueue extends AppenderSkeleton {

    private static final int QUEUE_SIZE = 30;
    public static final Queue<String> MESSAGE_QUEUE = EvictingQueue.create(QUEUE_SIZE);

    @Override
    protected void append(LoggingEvent event) {
        MESSAGE_QUEUE.offer(event.getRenderedMessage());
    }

    @Override
    public void close() {
        MESSAGE_QUEUE.clear();
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
