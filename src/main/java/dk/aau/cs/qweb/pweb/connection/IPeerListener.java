package dk.aau.cs.qweb.pweb.connection;

import dk.aau.cs.qweb.pweb.peer.Peer;
import dk.aau.cs.qweb.pweb.util.Fragment;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface IPeerListener {
    void start() throws IOException;
    void joinNetwork(UUID id, String address, int port);
    void requestInformation();
    void ping();
    void exchangeInformation(List<Peer> peers, List<Fragment> fragments);
    void addFragment(Fragment fragment, int ttl);
    void queryNetwork(String query, int ttl);
    void quit(UUID id);
    void reloadDataset() throws IOException;
}
