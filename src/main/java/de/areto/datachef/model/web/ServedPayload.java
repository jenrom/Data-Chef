package de.areto.datachef.model.web;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ServedPayload {

    private List<ServedNode> nodes = new ArrayList<>();
    private List<ServedEdge> edges = new ArrayList<>();

}
