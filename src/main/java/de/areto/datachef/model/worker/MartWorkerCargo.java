package de.areto.datachef.model.worker;

import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartType;
import de.areto.datachef.model.mart.TriggerMode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MartWorkerCargo extends WorkerCargo {

    @Enumerated(EnumType.STRING)
    private MartType martType;

    private boolean triggeredByCron;

    @OneToOne(fetch = FetchType.LAZY)
    private Mart mart;

}
