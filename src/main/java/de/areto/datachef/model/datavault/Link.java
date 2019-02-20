package de.areto.datachef.model.datavault;

import com.google.common.base.Joiner;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToMany;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

@Entity
@Getter
@Setter
public class Link extends DVObject {

    public static String buildLinkName(Collection<Hub> linkedHubs) {
        final List<String> sortedNames = linkedHubs.parallelStream()
                .map(Hub::getAliasName)
                .sorted()
                .collect(Collectors.toList());

        return Joiner.on("_").join(sortedNames);
    }

    private boolean selfReference = false;
    private boolean historicized = false;

    @OneToMany(mappedBy = "parentLink", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<Leg> legs = new HashSet<>();

    public Link() {
        super();
        this.setType(Type.LNK);
    }

    public Link(@NonNull String name) {
        this();
        this.setName(name);
    }

    public void addLeg(@NonNull Leg leg) {
        checkState(!hasLeg(leg.getRole(), leg.getHub()), "Leg already present");
        leg.setParentLink(this);
        this.legs.add(leg);
    }

    public boolean hasLeg(String role, Hub hub) {
        final Predicate<Leg> matchHubAndRole = l -> l.getRole().equals(role) && l.getHub().equals(hub);
        return legs.stream().anyMatch(matchHubAndRole);
    }

    public void setLegs(@NonNull Set<Leg> legs) {
        this.legs.clear();
        legs.forEach(this::addLeg);
    }

    public Collection<Leg> getLegsSorted() {
        return legs.stream().sorted().collect(Collectors.toList());
    }

    public Set<Hub> getLinkedHubs() {
        return legs.stream().map(Leg::getHub).collect(Collectors.toSet());
    }
}
