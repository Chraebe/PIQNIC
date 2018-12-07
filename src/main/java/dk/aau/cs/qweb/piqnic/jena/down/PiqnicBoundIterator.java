package dk.aau.cs.qweb.piqnic.jena.down;

import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PiqnicBoundIterator implements Iterator<Triple> {
    private final PiqnicDownTripleBindings bindings;
    private final Triple binding;
    private Triple nextTriple = null;

    public PiqnicBoundIterator(PiqnicDownTripleBindings bindings, Triple binding) {
        this.bindings = bindings;
        this.binding = binding;
    }

    private boolean match(Triple binding, Triple result) {
        if(!binding.getSubject().equals(Node.ANY) && !binding.getSubject().toString().equals(result.getSubject().toString()))
            return false;
        if(!binding.getPredicate().equals(Node.ANY) && !binding.getPredicate().toString().equals(result.getPredicate().toString()))
            return false;
        if(!binding.getObject().equals(Node.ANY) && !binding.getObject().toString().equals(result.getObject().toString()))
            return false;
        return true;
    }

    @Override
    public boolean hasNext() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        while(nextTriple == null) {
            if(!bindings.hasNext()) return false;
            Triple t = bindings.next();
            if(match(binding, t)) nextTriple = t;
        }
        return true;
    }

    @Override
    public Triple next() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if(!hasNext())
            throw new NoSuchElementException();
        Triple t = nextTriple;
        nextTriple = null;
        return t;
    }
}
