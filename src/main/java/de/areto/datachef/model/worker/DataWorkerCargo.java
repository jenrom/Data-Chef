package de.areto.datachef.model.worker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Inheritance;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Inheritance
public abstract class DataWorkerCargo extends WorkerCargo {
}
