package de.areto.datachef.model.common;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@MappedSuperclass
@Getter
@Setter
@NoArgsConstructor
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public abstract class IdentifiableEntity extends DefaultPrimaryKeyEntity {

    public static final String IDENTIFIER_COLUMN = "name";

    private static final Multimap<Class<?>, Field> NAT_FIELD_MAP = HashMultimap.create();

    @NaturalId(mutable = true)
    @Column(nullable = false)
    private String name;

    public void setName(String name) {
        checkArgument(name.length() <= 255, "Name exceeds 255 characters");
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;

        final Class otherClass = Hibernate.getClass(o);
        if (getClass() != otherClass) return false;

        for(Field field : findNaturalIds()) {
            try {
                final Method getterMethod = new PropertyDescriptor((String)field.getName(), getClass()).getReadMethod();
                final Object myFieldValue = getterMethod.invoke(this);
                final Object otherFieldValue = getterMethod.invoke(o);

                if(!Objects.equal(myFieldValue, otherFieldValue)) {
                    return false;
                }
            } catch (InvocationTargetException | IntrospectionException | IllegalAccessException e) {
                final String msg = String.format("Unable to compare equality of field '%s'", field);
                throw new IllegalStateException(msg);
            }

        }

        return true;
    }

    @Override
    public int hashCode() {
        final List<Field> fieldsSorted = findNaturalIds().stream()
                .sorted(Comparator.comparing(Field::getName))
                .collect(Collectors.toList());
        Collection<Object> valueCollection = new ArrayList<>(fieldsSorted.size());
        for(Field field : fieldsSorted) {
            try {
                final Method getterMethod = new PropertyDescriptor((String)field.getName(), getClass()).getReadMethod();
                final Object myFieldValue = getterMethod.invoke(this);
                valueCollection.add(myFieldValue);
            } catch (InvocationTargetException | IntrospectionException | IllegalAccessException e) {
                final String msg = String.format("Unable to calculate hash code of field '%s'", field);
                throw new IllegalStateException(msg);
            }

        }

        if(valueCollection.isEmpty()) {
            final String msg = String.format("%s has to have at leas one field annotated with '%s'",
                    getClass(), NaturalId.class);
            throw new IllegalStateException(msg);
        }

        return Objects.hashCode(valueCollection);
    }

    private Collection<Field> findNaturalIds() {
        if(!NAT_FIELD_MAP.containsKey(getClass())) {
            Class<?> type = getClass();
            while (type != null) {
                for (Field field : type.getDeclaredFields()) {
                    if(field.isAnnotationPresent(NaturalId.class)) {
                        NAT_FIELD_MAP.put(getClass(), field);
                    }
                }
                type = type.getSuperclass();
            }
        }

        return NAT_FIELD_MAP.get(getClass());
    }
}