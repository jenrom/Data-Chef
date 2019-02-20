package de.areto.datachef.model.mapping;

import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import de.areto.datachef.model.datavault.DVObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingObjectReference extends DefaultPrimaryKeyEntity implements Serializable, Comparable<MappingObjectReference> {

    public static final Comparator<MappingObjectReference> COMPARATOR = Comparator
            .comparing(MappingObjectReference::getObject)
            .thenComparing(MappingObjectReference::getRole);

    public static final String DEFAULT_ROLE = "#CHEF_DEFAULT";

    @ManyToOne(optional = false)
    @NaturalId
    private Mapping mapping;

    @ManyToOne(optional = false)
    @NaturalId
    private DVObject object;

    @NaturalId
    private String role = DEFAULT_ROLE;

    public MappingObjectReference(@NonNull Mapping mapping, @NonNull DVObject object) {
        this(mapping, object, DEFAULT_ROLE);
    }

    public MappingObjectReference(@NonNull Mapping mapping, @NonNull DVObject object, @NonNull String role) {
        this.mapping = mapping;
        this.object = object;
        this.role = role;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MappingObjectReference reference = (MappingObjectReference) o;
        return Objects.equals(mapping, reference.mapping) &&
                Objects.equals(object, reference.object) &&
                Objects.equals(role, reference.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapping, object, role);
    }

    @Override
    public int compareTo(MappingObjectReference other) {
        return COMPARATOR.compare(this, other);
    }

    public boolean isDefaultRole() {
        return role != null && role.equals(DEFAULT_ROLE);
    }
}
