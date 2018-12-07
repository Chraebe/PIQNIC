package dk.aau.cs.qweb.piqnic.connection;

public class PeerListenerFactory {
    public static IPeerListener createPeerListener(int port) {
        return new PeerListener(port);
    }
}
