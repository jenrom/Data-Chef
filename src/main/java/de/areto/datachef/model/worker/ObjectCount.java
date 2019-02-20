package de.areto.datachef.model.worker;

import de.areto.datachef.model.datavault.DVObject;
import lombok.Data;

@Data
public class ObjectCount {
    private final DVObject object;
    private final Long count;
}
