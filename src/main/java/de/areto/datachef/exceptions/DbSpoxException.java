package de.areto.datachef.exceptions;

public class DbSpoxException extends Exception {

    public DbSpoxException(String message) {
        super(message);
    }

    public DbSpoxException(String message, Throwable cause) {
        super(message, cause);
    }
}
