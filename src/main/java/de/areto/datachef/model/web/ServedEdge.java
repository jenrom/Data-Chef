package de.areto.datachef.model.web;

import lombok.Data;

@Data
public class ServedEdge {

    private long from;
    private long to;

    public ServedEdge(long from, long to) {
        this.from = from;
        this.to = to;
    }
}
