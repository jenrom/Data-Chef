package de.areto.datachef.model.worker;

import de.areto.datachef.model.mapping.StagingMode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingDataSQLWorkerCargo extends DataWorkerCargo {

    @Enumerated(EnumType.STRING)
    private StagingMode stagingMode;

}
