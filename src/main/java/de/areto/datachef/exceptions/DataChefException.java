package de.areto.datachef.exceptions;

public class DataChefException extends Exception {

    public DataChefException(String message) {
        super(message);
    }

    public DataChefException(String message, Throwable cause) {
        super(message, cause);
    }
}
