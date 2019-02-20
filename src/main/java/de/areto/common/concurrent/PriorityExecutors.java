package de.areto.common.concurrent;

import java.util.concurrent.*;

public class PriorityExecutors {

    public static ExecutorService newSingleThreadExecutor() {
        return getPriorityExecutor(1);
    }

    public static ThreadPoolExecutor getPriorityExecutor(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<>(10, PriorityFuture.COMP)) {

            @Override
            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                RunnableFuture<T> newTaskFor = super.newTaskFor(callable);
                return new PriorityFuture<>(newTaskFor, ((PriorityCallable<T>) callable).getPriority());
            }
        };
    }

}
