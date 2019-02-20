package de.areto.datachef.exceptions;

public class WebException extends Exception {

    public WebException(String message, Throwable cause) {
        super(message, cause);
    }

    public WebException(Throwable cause) {
        super(cause);
    }

    protected WebException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public WebException(String message) {
        super(message);
    }
}
