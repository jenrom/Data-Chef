package de.areto.common.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class StringUtility {

    public static String trimAndRemoveQuotes(String string) {
        if (string.startsWith("\"") && string.endsWith("\"")) {
            final String tmp = string.substring(1, string.length() - 1);
            return tmp.trim();
        } else {
            return string.trim();
        }
    }

    public static String millisToTimeString(long millis) {
        long second = (millis / 1000) % 60;
        long minute = (millis / (1000 * 60)) % 60;
        long hour = (millis / (1000 * 60 * 60)) % 24;

        return String.format("%02d:%02d:%02d.%d", hour, minute, second, millis);
    }

}
