package de.areto.common.concurrent;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PriorityFuture<T> implements RunnableFuture<T> {

    private RunnableFuture<T> src;
    private long priority;

    public PriorityFuture(RunnableFuture<T> other, long priority) {
        this.src = other;
        this.priority = priority;
    }

    public long getPriority() {
        return priority;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return src.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
        return src.isCancelled();
    }

    public boolean isDone() {
        return src.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
        return src.get();
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return src.get(timeout, unit);
    }

    public void run() {
        src.run();
    }

    public static Comparator<Runnable> COMP = (o1, o2) -> {
        if (o1 == null && o2 == null)
            return 0;
        else if (o1 == null)
            return -1;
        else if (o2 == null)
            return 1;
        else {
            long p1 = ((PriorityFuture<?>) o1).getPriority();
            long p2 = ((PriorityFuture<?>) o2).getPriority();

            return Long.compare(p1, p2);
        }
    };
}
