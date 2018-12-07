package dk.aau.cs.qweb.piqnic.jena.exceptions;

public abstract class PiqnicException extends Exception {
    /**
     *
     * @param cause
     */
    public PiqnicException(Throwable cause) {
        super(cause);
    }

    /**
     *
     * @param message
     */
    public PiqnicException(String message) {
        super("Whoops! Looks like there was an error - " + message);
    }
}
