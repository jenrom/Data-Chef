package de.areto.datachef.model.web;

import de.areto.datachef.model.datavault.DVObject;
import lombok.Data;
import lombok.NonNull;

@Data
public class ServedNode {

    public static final String COLOR_HUB = "#00a9e9";
    public static final String COLOR_LINK = "#ededed";
    public static final String COLOR_SAT = "#ff6a73";

    private long id;
    private String label;
    private String color;
    private String shape = "box";
    private String data;


    public ServedNode(@NonNull DVObject object) {
        if(!object.isTransient()) this.id = object.getDbId();
        this.label = object.getNamePrefix() + object.getName();
        this.data = object.getType().toString();

        if(object.isHub()) this.color = COLOR_HUB;

        if(object.isLink()) {
            this.color = COLOR_LINK;
            if(object.asLink().isHistoricized()) this.label += " H";
        }

        if(object.isSatellite()) this.color = COLOR_SAT;
    }
}
