package de.areto.datachef.model.web;

import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Satellite;
import lombok.Data;

import java.util.List;

@Data
public class ObjectDataBean<T extends DVObject> {

    private final T parent;
    private final List<Satellite> satelliteList;

}
