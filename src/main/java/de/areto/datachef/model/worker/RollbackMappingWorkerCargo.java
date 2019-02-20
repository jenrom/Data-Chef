package de.areto.datachef.model.worker;

import de.areto.datachef.model.mapping.Mapping;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Transient;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class RollbackMappingWorkerCargo extends RollbackWorkerCargo {

    @Transient
    private Mapping mapping;
}
