package de.areto.datachef.scheduler;

import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

public class Mousetrap {

    @Getter
    private final UUID id = UUID.randomUUID();

    @Getter
    private final String parentMappingName;

    private final Set<String> receivedMappingNames = new HashSet<>();

    @Getter
    private boolean stretched = false;

    private Instant stretchTime;


    private final Set<String> requiredMappingNames;
    private final Duration timeoutDuration;

    public Mousetrap(@NonNull String parentName, @NonNull Set<String> requiredNames, @NonNull Duration timeoutDuration) {
        checkState(!requiredNames.isEmpty(), "Provide at least one Mapping name");
        this.parentMappingName = parentName;
        this.requiredMappingNames = requiredNames;
        this.timeoutDuration = timeoutDuration;
    }

    public boolean canStrike() {
        if(!stretched)
            return false;

        return !isTimedOut() && receivedMappingNames.containsAll(requiredMappingNames);
    }

    public boolean isMappingNameRequired(@NonNull String mappingName) {
        return requiredMappingNames.contains(mappingName);
    }

    public boolean wasMappingNameReceived(@NonNull String mappingName) {
        return receivedMappingNames.contains(mappingName);
    }

    public boolean canReceive(@NonNull String mappingName) {
        return isMappingNameRequired(mappingName) && !wasMappingNameReceived(mappingName);
    }

    public void offerMappingName(@NonNull String mappingName) {
        if(!isMappingNameRequired(mappingName))
            throw new IllegalArgumentException("Mapping " + mappingName + " is not required");

        if(wasMappingNameReceived(mappingName))
            throw new IllegalArgumentException("Mapping " + mappingName + " was already received");

        if(receivedMappingNames.isEmpty()) {
            stretched = true;
            stretchTime = Instant.now();
        }

        this.receivedMappingNames.add(mappingName);
    }

    public boolean isEmpty() {
        return this.receivedMappingNames.isEmpty();
    }

    public boolean isTimedOut() {
        if(stretchTime == null) return false;

        final boolean timeout = Instant.now().isAfter(stretchTime.plus(timeoutDuration));
        return timeout && !receivedMappingNames.isEmpty();
    }

    public void reset() {
        receivedMappingNames.clear();
        stretchTime = Instant.now();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mousetrap mousetrap = (Mousetrap) o;
        return Objects.equals(id, mousetrap.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
