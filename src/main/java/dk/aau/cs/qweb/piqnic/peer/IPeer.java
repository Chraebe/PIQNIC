package dk.aau.cs.qweb.piqnic.peer;

import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface IPeer {
    List<IPeer> shuffle(List<IPeer> peers) throws IOException;

    String getAddress();

    int getPort();

    UUID getId();

    long estimateNumResults(UUID id, int ttl, Triple triple) throws IOException;

    void processTriplePattern(UUID id, int ttl, Triple triple, PrintWriter writer) throws IOException;

    void processTriplePatternBound(UUID id, int ttl, Triple triple, List<String> bindings, PrintWriter writer) throws IOException;

    void addFragmentForTest(FragmentBase fragment, int ttl, PrintWriter writer) throws IOException;
}
