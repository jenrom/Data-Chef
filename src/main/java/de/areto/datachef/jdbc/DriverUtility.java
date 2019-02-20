package de.areto.datachef.jdbc;

import de.areto.datachef.exceptions.DataChefException;
import lombok.experimental.UtilityClass;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;

@UtilityClass
public class DriverUtility {

    public static void loadExternalDriver(URL url, String className) throws DataChefException {
        try {
            URLClassLoader ucl = new URLClassLoader(new URL[]{url});
            Driver d = (Driver) Class.forName(className, true, ucl).newInstance();
            DriverManager.registerDriver(new DriverShim(d));
        } catch (Throwable parentThrowable) {
            final String message = String.format("Unable to load Driver '%s' at '%s'", className, url);
            throw new DataChefException(message, parentThrowable);
        }
    }
}


