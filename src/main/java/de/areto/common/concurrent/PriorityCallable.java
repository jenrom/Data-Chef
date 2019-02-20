package de.areto.common.concurrent;

import java.util.concurrent.Callable;

public interface PriorityCallable<T> extends Callable<T> {

    long getPriority();

}