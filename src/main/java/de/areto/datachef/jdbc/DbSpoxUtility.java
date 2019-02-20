package de.areto.datachef.jdbc;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
public class DbSpoxUtility {

    private final static String SQL_TYPE_PATTERN = "([a-zA-z_]+)(\\({1}([\\d]+|[\\d]+,[\\d]+)\\){1})?";

    public static String[] parseSqlType(@NonNull String sqlType) {
        final String s = sqlType.replaceAll("\\s+", "");
        final Matcher m = Pattern.compile(SQL_TYPE_PATTERN).matcher(s);

        if (!m.find()) return new String[0];

        final String parts[] = new String[3];

        if (s.endsWith(")")) {
            parts[0] = s.substring(0, s.indexOf("("));

            String inBraces = s.substring(s.indexOf("(") + 1, s.indexOf(")"));
            if (inBraces.contains(",")) {
                String precScale[] = inBraces.split(",");
                parts[1] = precScale[0];
                parts[2] = precScale[1];
            } else {
                parts[1] = inBraces;
                parts[2] = null;
            }
        } else {
            parts[0] = s;
            parts[1] = null;
            parts[2] = null;
        }

        parts[0] = parts[0].toLowerCase();

        return parts;
    }

    public static String escapeSingleQuotes(@NonNull String s) {
        final StringBuilder sb = new StringBuilder(s);
        int escapes = 0;

        for (int i = 0; i < s.length(); i++) {
            boolean escaped = false;
            boolean isSingleQuote = s.charAt(i) == '\'';

            if (isSingleQuote && i > 0 && s.charAt(i - 1) == '\'')
                escaped = true;

            if (isSingleQuote && i + 1 < s.length() && s.charAt(i + 1) == '\'')
                escaped = true;

            if (isSingleQuote && !escaped) {
                sb.insert(i + escapes, '\'');
                escapes++;
            }
        }
        return sb.toString();
    }

}