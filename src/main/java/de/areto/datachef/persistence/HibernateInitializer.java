package de.areto.datachef.persistence;

import lombok.NonNull;
import org.hibernate.Hibernate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class HibernateInitializer {

    public static void initialize(@NonNull Object o) {
        initialize(o, new HashSet<>());
    }

    private static void initialize(@NonNull Object o, @NonNull Set<Object> seenObjects) {
        seenObjects.add(o);

        for (Method method : o.getClass().getMethods()) {

            String methodName = method.getName();

            // check Getters exclusively
            if (methodName.length() < 3 || !"get".equals(methodName.substring(0, 3)))
                continue;

            // Getters without parameters
            if (method.getParameterTypes().length > 0)
                continue;

            int modifiers = method.getModifiers();

            if (Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers))
                continue;

            try {
                final Object r = method.invoke(o);

                if (r == null) continue;

                if (seenObjects.contains(r)) continue; // Cycles...

                if (isIgnoredType(r.getClass()) || r.getClass().isPrimitive() || r.getClass().isArray() || r.getClass().isAnonymousClass())
                    continue;

                // Initialize if Proxy
                Hibernate.initialize(r);

                if(r instanceof Collection) {
                    Collection collection = (Collection) r;
                    for(Object child : collection)
                        initialize(child, seenObjects);
                } else {
                    initialize(r, seenObjects);
                }
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private static final Set<Class<?>> IGNORED_TYPES = getIgnoredTypes();

    private static boolean isIgnoredType(Class<?> clazz) {
        return IGNORED_TYPES.contains(clazz);
    }

    private static Set<Class<?>> getIgnoredTypes() {
        final Set<Class<?>> ret = new HashSet<>();
        ret.add(Boolean.class);
        ret.add(Character.class);
        ret.add(Byte.class);
        ret.add(Short.class);
        ret.add(Integer.class);
        ret.add(Long.class);
        ret.add(Float.class);
        ret.add(Double.class);
        ret.add(Void.class);
        ret.add(String.class);
        ret.add(Class.class);
        ret.add(Package.class);
        return ret;
    }
}
