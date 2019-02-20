package de.areto.datachef.model.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.Objects;

/**
 * <p>
 *     Abstract JPA {@link Entity} that is identified by a sequentially
 *     generated numerical Primary Key {@link DefaultPrimaryKeyEntity#dbId}.
 * </p>
 * <p>
 *     <strong>Attention:</strong> {@link DefaultPrimaryKeyEntity#dbId} is not set, e.g. is null, until the instance
 *     is persisted and thereby managed by JPA. It is not safe to collect transient instances of
 *     {@link DefaultPrimaryKeyEntity} in a {@link java.util.Set} or {@link java.util.Map} because
 *     {@link DefaultPrimaryKeyEntity#hashCode()} will change as soon as the instance is persisted.
 *     Once {@link DefaultPrimaryKeyEntity#hashCode()} changes the instance cannot be located in a
 *     {@link java.util.Set} or {@link java.util.Map} anymore. See
 *     <a href="https://goo.gl/8nT977">the dicussion on Stackoverflow.com</a> and
 *     <a href="https://goo.gl/vLbeuU">Vlad's blog</a> for more information.
 * </p>
 * <p>
 *     {@link DefaultPrimaryKeyEntity#dbId} will be generation using {@link GenerationType#SEQUENCE} promising better
 *     performance. See <a href="https://goo.gl/LxgmKn">Vlad's blog</a> for a detailed explanation.
 * </p>
 */
@MappedSuperclass
@Getter
@Setter
@NoArgsConstructor
public abstract class DefaultPrimaryKeyEntity {

    /**
     * A constant holding the name of the field "dbId". This can be used in programmatically created queries.
     */
    public static final String ID_COLUMN = "dbId";

    /**
     * A sequentially generated identifier used as database tables primary key.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(updatable = false, nullable = false)
    private Long dbId;

    public boolean isTransient() {
        return dbId == null;
    }

    @Override
    public String toString() {
        final String className = this.getClass().getSimpleName();
        return String.format("%s{%s}", className, isTransient() ? "transient" : dbId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (dbId == null) return false;
        DefaultPrimaryKeyEntity that = (DefaultPrimaryKeyEntity) o;
        return Objects.equals(dbId, that.getDbId());
    }

    @Override
    public int hashCode() {
        return dbId == null ? super.hashCode() : Objects.hash(dbId);
    }

}