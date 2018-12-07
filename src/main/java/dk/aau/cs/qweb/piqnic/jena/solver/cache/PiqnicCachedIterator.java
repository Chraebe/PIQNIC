package dk.aau.cs.qweb.piqnic.jena.solver.cache;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.List;
import java.util.NoSuchElementException;

public class PiqnicCachedIterator extends NiceIterator<Triple> {
    private List<Triple> triples;
    private int currentPosition = 0;

    public PiqnicCachedIterator(List<Triple> triples) {
        this.triples = triples;
    }

    @Override
    public boolean hasNext() {
        return triples.size() > currentPosition;
    }

    @Override
    public Triple next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Triple t = triples.get(currentPosition);
        currentPosition++;
        return t;
    }
}
