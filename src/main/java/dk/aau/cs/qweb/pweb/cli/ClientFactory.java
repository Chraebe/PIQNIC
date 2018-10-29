package dk.aau.cs.qweb.pweb.cli;

public class ClientFactory {
    public static IClient createClient(int port) {
        return new Client(port);
    }
}
