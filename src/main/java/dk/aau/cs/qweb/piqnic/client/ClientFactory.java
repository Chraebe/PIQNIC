package dk.aau.cs.qweb.piqnic.client;

import dk.aau.cs.qweb.piqnic.test.TestClient;

public class ClientFactory {
    public static IClient createClient(int port) {
        return new CLI(port);
    }
    public static IClient createTestClient(int port) {
        return new TestClient(port);
    }
}
