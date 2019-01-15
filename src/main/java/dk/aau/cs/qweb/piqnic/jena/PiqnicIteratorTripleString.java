package dk.aau.cs.qweb.piqnic.jena;

import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.List;

public class PiqnicIteratorTripleString implements IteratorTripleString {
    private List<TripleString> triples;
    private int index = 0;
    public PiqnicIteratorTripleString(List<TripleString> triples) {
        this.triples = triples;
    }

    @Override
    public boolean hasPrevious() {
        return index > 0;
    }

    @Override
    public TripleString previous() {
        if(hasPrevious()) {
            index--;
            return triples.get(index);
        }
        return triples.get(0);
    }

    @Override
    public void goToStart() {
        index = 0;
    }

    @Override
    public long estimatedNumResults() {
        return triples.size();
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return ResultEstimationType.EXACT;
    }

    @Override
    public boolean hasNext() {
        return index < triples.size();
    }

    @Override
    public TripleString next() {
        if(hasNext()) {
            TripleString ret = triples.get(index);
            index++;
            return ret;
        }
        return null;
    }
}
