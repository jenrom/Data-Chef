package de.areto.datachef.exceptions;

public class CreatorException extends Exception {

    public CreatorException(String message) {
        super(message);
    }

    public CreatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public CreatorException(Throwable cause) {
        super(cause);
    }
}
