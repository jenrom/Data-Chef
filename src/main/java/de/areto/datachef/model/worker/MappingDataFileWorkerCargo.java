package de.areto.datachef.model.worker;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingDataFileWorkerCargo extends DataWorkerCargo {

    private String fileName;
    private String fileGroup;
    private LocalDateTime publishDate;
    private Long dataSize;

}
