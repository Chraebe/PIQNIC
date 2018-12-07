package dk.aau.cs.qweb.piqnic.node;

import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface INode {
    void addDataset(Dataset dataset);

    void addFragment(FragmentBase fragment);

    void addNeighbour(IPeer peer);

    void addNeighbours(List<IPeer> peers);

    void addFragments(List<FragmentBase> fragments) throws IOException;

    void shuffle() throws IOException;

    int getPort();

    String getIp();

    UUID getId();

    List<FragmentBase> getAllFragments();

    List<IPeer> getRandomPeers(int num);

    List<IPeer> getNeighbours();

    int fragmentCount();

    void removePeer(IPeer peer);

    void processTriplePattern(Triple triple, PrintWriter writer);

    void processTriplePatternBound(Triple triple, List<Map<String, String>> bindings, PrintWriter writer);

    long estimateResult(Triple triple);

    boolean insertFragment(FragmentBase fragmentBase);

    List<Dataset> getDatasets();
}
