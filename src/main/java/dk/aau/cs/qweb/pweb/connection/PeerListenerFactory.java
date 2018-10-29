package dk.aau.cs.qweb.pweb.connection;

public class PeerListenerFactory {
    public static IPeerListener createPeerListener(int port) {
        return new PeerListener(port);
    }
}
