package de.areto.datachef.model.worker;

import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingWorkerCargo extends WorkerCargo {

    @OneToOne(fetch = FetchType.LAZY)
    private Mapping mapping;

    @Enumerated(value = EnumType.STRING)
    private StagingMode stagingMode;

}