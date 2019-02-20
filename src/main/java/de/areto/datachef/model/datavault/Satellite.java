package de.areto.datachef.model.datavault;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

@Entity
@Getter
@Setter
public class Satellite extends DVObject implements Serializable {

    @ManyToOne(fetch = FetchType.LAZY)
    private DVObject parent;

    private boolean historySatellite = false;

    public Satellite() {
        super();
        this.setType(Type.SAT);
    }

    public Satellite(@NonNull String satName) {
        this();
        this.setName(satName);
    }

    public void setParent(@NonNull DVObject parent) {
        checkArgument(!parent.isSatellite(), "Parent DVObject must be a Hub or a Link");
        this.parent = parent;
    }
}
