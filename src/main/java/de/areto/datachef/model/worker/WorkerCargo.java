package de.areto.datachef.model.worker;

import de.areto.datachef.config.Constants;
import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import de.areto.datachef.model.compilation.SQLExpressionExecution;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Inheritance
@DiscriminatorColumn(name = "cargoType")
public abstract class WorkerCargo extends DefaultPrimaryKeyEntity {

    public enum Status {
        OKAY, ERROR, WARNING, REJECTED
    }

    @Column(updatable = false, insertable = false)
    private String cargoType;

    private LocalDateTime executionStartDateTime;
    private LocalDateTime executionEndDateTime;

    private long runtime;

    @Enumerated(EnumType.STRING)
    private Status status;

    private String name;

    private String checkSum;

    private Long payloadSize = 0L;

    @ElementCollection(fetch = FetchType.LAZY)
    @Column(length = Constants.CARGO_MSG_SIZE)
    private List<String> errors = new ArrayList<>();

    @ElementCollection(fetch = FetchType.LAZY)
    @Column(length = Constants.CARGO_MSG_SIZE)
    private List<String> warnings = new ArrayList<>();

    @ElementCollection(fetch = FetchType.LAZY)
    @Column(length = Constants.CARGO_MSG_SIZE)
    private List<String> messages = new ArrayList<>();

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "cargo", fetch = FetchType.LAZY)
    private List<SQLExpressionExecution> executions = new ArrayList<>();

    public void addExecution(SQLExpressionExecution execution) {
        execution.setCargo(this);
        execution.setStmntOrder(this.executions.size() + 1);
        this.executions.add(execution);
    }

    public boolean hasExecutions() {
        return !executions.isEmpty();
    }

    public void addError(String error) {
        addTrimmedMessage(errors, error);
    }

    public void addWarning(String warning) {
        addTrimmedMessage(warnings, warning);
    }

    public void addMessage(String message) {
        addTrimmedMessage(messages, message);
    }

    private void addTrimmedMessage(@NonNull List<String> collection, @NonNull String msg) {
        if(msg.length() > Constants.CARGO_MSG_SIZE) {
            final String subError = msg.substring(0, Constants.CARGO_MSG_SIZE - 3) + "...";
            collection.add(subError);
        } else {
            collection.add(msg);
        }
    }

    public void setExecutionEndDateTime(LocalDateTime executionEndDateTime) {
        this.executionEndDateTime = executionEndDateTime;
        this.runtime = Duration.between(this.executionStartDateTime, this.executionEndDateTime).toMillis();
    }

    public boolean hasMessages() { return !this.messages.isEmpty(); }

    public boolean hasErrors() {
        return !this.errors.isEmpty();
    }

    public boolean hasWarnings() {
        return !this.warnings.isEmpty();
    }

}