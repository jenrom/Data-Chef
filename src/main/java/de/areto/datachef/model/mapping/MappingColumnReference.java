package de.areto.datachef.model.mapping;

import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import de.areto.datachef.model.datavault.DVColumn;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingColumnReference extends DefaultPrimaryKeyEntity implements Serializable {

    public final static String DEFAULT_ROLE = "#default";

    @ManyToOne(optional = false)
    @NaturalId
    private MappingColumn mappingColumn;

    @ManyToOne(optional = false)
    @NaturalId
    private DVColumn dvColumn;

    @NaturalId
    private String role = DEFAULT_ROLE;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MappingColumnReference that = (MappingColumnReference) o;
        return Objects.equals(mappingColumn, that.mappingColumn) &&
                Objects.equals(dvColumn, that.dvColumn) &&
                Objects.equals(role, that.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappingColumn, dvColumn, role);
    }
}
