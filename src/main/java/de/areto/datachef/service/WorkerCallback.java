package de.areto.datachef.service;

import com.google.common.util.concurrent.FutureCallback;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.model.worker.WorkerInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@Slf4j
public abstract class WorkerCallback implements FutureCallback<WorkerCargo> {

    @Getter
    private final WorkerInfo workerInfo;

    public WorkerCallback(WorkerInfo workerInfo) {
        this.workerInfo = workerInfo;
    }

    public void workerCallback() {};

    public void handleSuccess() {}

    public void handleSuccessResult(WorkerCargo result) {}

    public void handleFailure(Throwable throwable) {};

    @Override
    public void onSuccess(@Nullable WorkerCargo result) {
        workerCallback();
        if (result != null) {
            handleSuccessResult(result);
        } else {
            handleSuccess();
        }
    }

    @Override
    public void onFailure(Throwable throwable) {
        workerCallback();
        handleFailure(throwable);
    }
}
