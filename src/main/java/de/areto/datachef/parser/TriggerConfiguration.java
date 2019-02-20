package de.areto.datachef.parser;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class TriggerConfiguration {
    private boolean cronTrigger;
    private int timeout;
    private String timeoutUnit;
    private String cronExpression;
    private Set<String> dependencies = new HashSet<>();
}
