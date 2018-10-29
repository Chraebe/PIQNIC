package dk.aau.cs.qweb.pweb.node;

import dk.aau.cs.qweb.pweb.peer.IPeer;
import dk.aau.cs.qweb.pweb.peer.Peer;
import dk.aau.cs.qweb.pweb.util.Dataset;
import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface INode {
    void addDataset(Dataset dataset) throws IOException;
    void addFragment(Fragment fragment) throws IOException;
    void addPeer(IPeer peer);
    void addPeers(List<IPeer> peers);
    void addFragments(List<Fragment> fragments) throws IOException;
    QueryResult processQuery(String sparql);
    int getPort();
    String getIp();
    UUID getId();
    List<Fragment> getRandomFragments(int num);
    List<Peer> getRandomPeers(int num);
    List<IPeer> getPeers();
    void reloadDatastore() throws IOException;
    int fragmentCount();
}
