package de.areto.datachef.model.worker;

import de.areto.datachef.model.mart.Mart;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Transient;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class RollbackMartWorkerCargo extends RollbackWorkerCargo {

    @Transient
    private Mart mart;
}
