package de.areto.datachef.model.datavault;

import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.Hibernate;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class Leg extends DefaultPrimaryKeyEntity implements Serializable, Comparable<Leg> {

    private static final Comparator<Leg> COMPARATOR = Comparator.comparing(Leg::getHubName).thenComparing(Leg::getRole);

    public final static String DEFAULT_ROLE = "#default";

    @ManyToOne(fetch = FetchType.LAZY)
    @NaturalId
    private Link parentLink;

    @OneToOne
    @NaturalId
    private Hub hub;

    @NaturalId
    private String role = DEFAULT_ROLE;

    private boolean driving = false;

    public Leg(Hub hub, boolean driving) {
        this.hub = hub;
        this.driving = driving;
    }

    public String getHubName() {
        return hub.getName();
    }

    public boolean isDefaultRole() {
        return role.equals(DEFAULT_ROLE);
    }

    @Override
    public String toString() {
        return String.format("Leg{%s%s}", hub, driving ? " (driving)" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        final Class otherClass = Hibernate.getClass(o);
        if (getClass() != otherClass) return false;

        Leg leg = (Leg) o;

        return Objects.equals(parentLink, leg.parentLink) &&
                Objects.equals(hub, leg.hub) &&
                Objects.equals(role, leg.role);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentLink, hub, role);
    }

    @Override
    public int compareTo(Leg o) {
        return COMPARATOR.compare(this, o);
    }
}
