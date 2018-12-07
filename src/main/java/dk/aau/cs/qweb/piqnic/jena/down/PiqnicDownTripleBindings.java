package dk.aau.cs.qweb.piqnic.jena.down;


import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class PiqnicDownTripleBindings {
    private final Triple triple;
    private List<Triple> bindings = new ArrayList<>();
    int currentPos = 0;

    public PiqnicDownTripleBindings(Triple triple) {
        this.triple = triple;
    }

    public void put(Triple binding) {
        bindings.add(binding);
    }

    public Triple get(int i) {
        return bindings.get(i);
    }

    public void removeDuplicates() {
        bindings = new ArrayList<>(new HashSet<>(bindings));
    }

    public void reset() {
        currentPos = 0;
    }

    public boolean hasNext() {
        return currentPos < bindings.size();
    }

    public Triple next() {
        Triple t = bindings.get(currentPos);
        ++currentPos;
        return t;
    }

    public void delete() {
        bindings.clear();
    }
}
