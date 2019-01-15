package dk.aau.cs.qweb.piqnic.peer;

import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.util.Constituents;
import dk.aau.cs.qweb.piqnic.util.Triple;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface IPeer {
    List<Peer> shuffle(List<Peer> peers) throws IOException;

    String getAddress();

    int getPort();

    UUID getId();

    long estimateNumResults(UUID id, int ttl, Triple triple) throws IOException;

    void processTriplePattern(UUID id, int ttl, Triple triple, PrintWriter writer) throws IOException;

    void processTriplePatternBound(UUID id, int ttl, Triple triple, List<String> bindings, PrintWriter writer) throws IOException;

    void addFragmentForTest(FragmentBase fragment, int ttl, PrintWriter writer) throws IOException;

    void addFragment(FragmentBase fragment, List<TripleString> triples, int ttl, PrintWriter writer) throws IOException;

    Set<IPeer> addFragmentInit(FragmentBase fragment, List<TripleString> triples, int ttl) throws IOException;

    Constituents getConstituents() throws IOException;

    void passJoin(Peer p) throws IOException;

    void join(Peer p) throws IOException;

    void addTriplesToFragment(FragmentBase fragment, List<Triple> triples) throws IOException;

    void removeTriplesFromFragment(FragmentBase fragment, List<Triple> triples) throws IOException;
}
