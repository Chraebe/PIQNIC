package dk.aau.cs.qweb.piqnic.jena.exceptions;

public class ConnectionNotMadeException extends PiqnicException {
    public ConnectionNotMadeException(Throwable cause) {
        super(cause);
    }

    public ConnectionNotMadeException(String message) {
        super(message);
    }
}
