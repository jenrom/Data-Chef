package de.areto.datachef.model.datavault;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.Entity;
import java.io.Serializable;

@Entity
@Getter
@Setter
public class Hub extends DVObject implements Serializable {

    private String alias;

    public Hub() {
        super.setType(Type.HUB);
    }

    public Hub(@NonNull String name) {
        this();
        this.setName(name);
    }

    @Override
    public void addColumn(DVColumn c) {
        c.setKeyColumn(true);
        super.addColumn(c);
    }

    public String getAliasName() {
        return hasAlias() ? alias : getName();
    }

    public boolean hasAlias() {
        return alias != null && !alias.equals("");
    }
}
