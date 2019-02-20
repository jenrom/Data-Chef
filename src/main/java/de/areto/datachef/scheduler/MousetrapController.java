package de.areto.datachef.scheduler;

import lombok.NonNull;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MousetrapController {

    private final Map<String, Mousetrap> trapMap = Collections.synchronizedMap(new HashMap<>());

    private final DataWorkerTrigger workerTrigger;

    public MousetrapController(@NonNull DataWorkerTrigger workerTrigger) {
        this.workerTrigger = workerTrigger;
    }

    public Mousetrap createTrap(@NonNull String parentName, @NonNull Set<String> requiredNames, @NonNull Duration timeout) {
        if(trapMap.containsKey(parentName)) {
            final String msg = String.format("Trap for %s already created", parentName);
            throw new IllegalArgumentException(msg);
        }

        final Mousetrap mousetrap = new Mousetrap(parentName, requiredNames, timeout);
        trapMap.put(parentName, mousetrap);
        return mousetrap;
    }

    public boolean contains(@NonNull String parentName) {
        return trapMap.containsKey(parentName);
    }

    public void notify(@NonNull String dataMappingName) {
        synchronized (trapMap) {
            for (Mousetrap trap : trapMap.values()) {
                if (trap.canReceive(dataMappingName)) {
                    trap.offerMappingName(dataMappingName);

                    if (trap.canStrike()) {
                        triggerDataWorker(trap.getParentMappingName());
                        trap.reset();
                    }
                }
            }
        }
    }

    public void checkTraps() {
        synchronized (trapMap) {
            for (Mousetrap trap : trapMap.values()) {
                if (trap.isTimedOut()) {
                    trap.reset();
                    triggerDataWorker(trap.getParentMappingName());
                }
            }
        }
    }

    public void removeTrap(@NonNull String parentName) {
        trapMap.remove(parentName);
    }

    public void clear() {
        this.trapMap.clear();
    }

    private void triggerDataWorker(@NonNull String unitName) {
        workerTrigger.trigger(unitName);
    }
}
