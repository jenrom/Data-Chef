package de.areto.datachef.model.worker;

import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.worker.Worker;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@Getter
@Setter
public class WorkerInfo {

    private final UUID runId;
    private final Class<? extends Worker> workerClass;
    private final String name;
    private final String fileName;
    private final StagingMode stagingMode;
    private LocalDateTime queuedDateTime;

    public WorkerInfo(UUID runId, Class<? extends Worker> workerClass, String name, String fileName, StagingMode stagingMode, LocalDateTime queuedDateTime) {
        this.runId = runId;
        this.workerClass = workerClass;
        this.name = name;
        this.fileName = fileName;
        this.stagingMode = stagingMode;
        this.queuedDateTime = queuedDateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerInfo that = (WorkerInfo) o;
        return Objects.equals(runId, that.runId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId);
    }

    @Override
    public String toString() {
        return "WorkerInfo{" +
                "runId=" + runId +
                ", workerClass=" + workerClass +
                ", name='" + name + '\'' +
                '}';
    }

    public static class Builder {
        private UUID runId;
        private Class<? extends Worker> workerClass;
        private String name;
        private String fileName;
        private LocalDateTime queuedDateTime;
        private StagingMode stagingMode;

        public Builder setRunId(UUID runId) {
            this.runId = runId;
            return this;
        }

        public Builder setStagingMode(StagingMode stagingMode) {
            this.stagingMode = stagingMode;
            return this;
        }

        public Builder setQueuedDateTime(LocalDateTime queuedDateTime) {
            this.queuedDateTime = queuedDateTime;
            return this;
        }

        public Builder setWorkerClass(Class<? extends Worker> workerClass) {
            this.workerClass = workerClass;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public WorkerInfo create() {
            return new WorkerInfo(runId, workerClass, name, fileName, stagingMode, queuedDateTime);
        }
    }
}
