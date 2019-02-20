package de.areto.datachef.model.datavault;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import de.areto.datachef.config.ConfigUtility;
import de.areto.datachef.config.Constants;
import de.areto.datachef.model.common.IdentifiableEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.abego.treelayout.internal.util.Contract.checkState;

@Entity
@Inheritance
@Getter
@Setter
@NoArgsConstructor
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
public abstract class DVObject extends IdentifiableEntity implements Comparable<DVObject> {

    public static final Comparator<DVObject> COMPARATOR = Comparator.comparing(DVObject::getType).thenComparing(DVObject::getName);
    public static final String TYPE_COLUMN = "type";

    public enum Type {
        HUB,
        LNK,
        SAT
    }

    @NaturalId
    @Enumerated(EnumType.STRING)
    private Type type;

    @OneToMany(mappedBy = "parent", fetch = FetchType.LAZY, cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<DVColumn> columns = new HashSet<>();

    @ElementCollection(fetch = FetchType.LAZY)
    @Column(length = Constants.DV_MAX_CMNT_SIZE)
    private Set<String> comments = new HashSet<>();

    public void addColumn(@NonNull DVColumn c) {
        c.setParent(this);
        this.columns.add(c);
    }

    public void setColumns(@NonNull Set<DVColumn> columns) {
        this.columns.clear();
        columns.forEach(this::addColumn);
    }

    public DVColumn getColumnByName(@NonNull String name) {
        final Predicate<DVColumn> byName = c -> c.getName().equals(name);
        final Optional<DVColumn> result = columns.stream().filter(byName).findAny();
        return result.orElse(null);
    }

    public boolean hasColumn(@NonNull String colName) {
        final Predicate<String> namePredicate = n -> n.equals(colName);
        return columns.stream().map(DVColumn::getName).anyMatch(namePredicate);
    }

    public List<DVColumn> getColumnsSorted() {
        return columns.stream().sorted(Comparator.comparing(DVColumn::getName)).collect(Collectors.toList());
    }

    public boolean isHub() {
        return type != null && type.equals(Type.HUB);
    }

    public boolean isLink() {
        return type != null && type.equals(Type.LNK);
    }

    public boolean isSatellite() {
        return type != null && type.equals(Type.SAT);
    }

    public void addComment(@NonNull String comment) {
        checkArgument(comment.length() <= Constants.DV_MAX_CMNT_SIZE,
                "Comment is longer than %s characters", Constants.DV_MAX_CMNT_SIZE);
        this.comments.add(comment);
    }

    public Satellite asSatellite() {
        checkState(this.isSatellite(), "Object must be a Satellite");
        return castMe(Satellite.class);
    }

    public Hub asHub() {
        checkState(this.isHub(), "Object must be a Hub");
        return castMe(Hub.class);
    }

    public Link asLink() {
        checkState(this.isLink(), "Object must be a Link");
        return castMe(Link.class);
    }

    private <T extends DVObject> T castMe(Class<T> toClass) {
        if (toClass.isAssignableFrom(this.getClass())) {
            return toClass.cast(this);
        } else {
            final String msg = String.format("DVObject cannot be casted to '%s' because it's of type '%s'",
                    toClass, getClass());
            throw new RuntimeException(msg);
        }
    }

    public String getKeySuffix() {
        return ConfigUtility.getKeySuffix(this);
    }

    public String getNamePrefix() {
        return ConfigUtility.getNamePrefix(this);
    }

    public boolean hasComment() {
        return !this.comments.isEmpty();
    }

    @Override
    public String toString() {
        return "(" + type + ") " + getName();
    }

    @Override
    public int compareTo(@NonNull DVObject other) {
        return COMPARATOR.compare(this, other);
    }
}