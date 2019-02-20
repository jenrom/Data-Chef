package de.areto.common.util;

import lombok.experimental.UtilityClass;

import java.util.stream.Stream;

@UtilityClass
public class ReflectionUtility {

    public static boolean hasNoArgsConstructor(Class<?> clazz) {
        return Stream.of(clazz.getConstructors()).anyMatch((c) -> c.getParameterCount() == 0);
    }

}
