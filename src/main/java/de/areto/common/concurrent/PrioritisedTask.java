package de.areto.common.concurrent;

import lombok.Getter;

@Getter
public class PrioritisedTask<T extends Runnable> implements Runnable, Comparable<PrioritisedTask> {

    private final String description;
    private final Long priority;
    private final T childTask;

    public PrioritisedTask(String description, T childTask) {
        this(description, System.nanoTime(), childTask);
    }

    public PrioritisedTask(String description, Long priority, T childTask) {
        this.description = description;
        this.priority = priority;
        this.childTask = childTask;
    }

    @Override
    public void run() {
        childTask.run();
    }

    @Override
    public int compareTo(PrioritisedTask o) {
        return priority.compareTo(o.priority);
    }
}
