package dk.aau.cs.qweb.piqnic.jena.solver.cache;

import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicJenaFloodIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PiqnicCache {
    private Map<Triple, List<Triple>> iterators = new HashMap<>();
    private Map<Node, List<Triple>> predIterators = new HashMap<>();

    public boolean isCached(Triple triple) {
        return iterators.containsKey(triple) || predIterators.containsKey(triple.getPredicate());
    }

    public void put(Triple triple, List<Triple> iterator) {
        if(isCached(triple)) return;
        //if(triple.getSubject().equals(Node.ANY) && triple.getObject().equals(Node.ANY)) {
        //    predIterators.put(triple.getPredicate(), iterator);
        //} else {
            iterators.put(triple, iterator);
        //}
    }

    public NiceIterator<Triple> get(Triple triple) {
        if(iterators.containsKey(triple)) {
            return new PiqnicCachedIterator(iterators.get(triple));
        } //else if (predIterators.containsKey(triple.getPredicate())) {
        //    return new PiqnicPredicateSpecificCachedIterator(new PiqnicCachedIterator(predIterators.get(triple.getPredicate())), triple);
        //}
        return PiqnicJenaFloodIterator.emptyIterator();
    }

    public void destroy() {
        iterators.clear();
        predIterators.clear();
    }
}
