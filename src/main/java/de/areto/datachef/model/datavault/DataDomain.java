package de.areto.datachef.model.datavault;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Getter
@Setter
public class DataDomain implements Serializable {

    @Id
    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String sqlType;

    private String defaultValue;

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataDomain domain = (DataDomain) o;
        return Objects.equals(name, domain.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DataDomain{" + name + '}';
    }
}
