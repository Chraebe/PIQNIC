package dk.aau.cs.qweb.piqnic.jena.solver.cache;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicJenaFloodIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;

public class PiqnicPredicateSpecificCachedIterator extends NiceIterator<Triple> {
    private final NiceIterator<Triple> iterator;
    private Triple nextTriple = null;
    private Node subject;
    private Node object;

    public PiqnicPredicateSpecificCachedIterator(NiceIterator<Triple> iterator, Triple triple) {
        this.iterator = iterator;
        subject = triple.getSubject();
        object = triple.getObject();
        System.out.println(triple.toString());
    }

    private Triple bufferNext() {
        while(iterator.hasNext()) {
            Triple t = iterator.next();
            System.out.println(t.toString());
            if((subject.equals(Node.ANY) || subject.equals(t.getSubject())) &&
                    (object.equals(Node.ANY) || object.equals(t.getObject()))) {
                nextTriple = t;
                return nextTriple;
            }
        }
        System.out.println("The end!");
        return null;
    }

    @Override
    public boolean hasNext() {
        return nextTriple != null || bufferNext() != null;
    }

    @Override
    public Triple next() {
        if (!hasNext())
            throw new NoSuchElementException();

        if(PiqnicClient.test) PiqnicJenaConstants.NTB++;

        Triple t = nextTriple;
        nextTriple = null;
        //System.out.println(t.toString());
        return t;
    }
}
