package de.areto.datachef.model.worker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Inheritance;

@Entity
@Getter
@Setter
@Inheritance
@NoArgsConstructor
public abstract class RollbackWorkerCargo extends WorkerCargo {

    private String rollbackType;

}
