package de.areto.datachef.model.worker;

import de.areto.datachef.model.mart.MartType;
import de.areto.datachef.model.mart.TriggerMode;
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
public class MartDataWorkerCargo extends DataWorkerCargo {

    @Enumerated(EnumType.STRING)
    private MartType martType;

    @Enumerated(EnumType.STRING)
    private TriggerMode triggerMode;

}
