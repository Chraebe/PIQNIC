package dk.aau.cs.qweb.pweb.node;

import dk.aau.cs.qweb.pweb.util.Dataset;
import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;
import dk.aau.cs.qweb.pweb.util.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.commons.lang3.StringEscapeUtils.escapeHtml4;

public class PWebNode extends AbstractNode {
    public HDT hdt;
    File f;
    private int ttl = 5;

    public PWebNode(String ip, int port) throws IOException {
        super(ip, port);
        f = File.createTempFile(this.id.toString(), ".hdt");

        try {
            ArrayList<TripleString> ts = new ArrayList<>();
            ts.add(new TripleString("http://qweb.cs.aau.dk/pweb/node/" + id.toString(), "http://qweb.cs.aau.dk/pweb/metadata/id", id.toString()));
            hdt = HDTManager.generateHDT(ts.iterator(), "http://qweb.cs.aau.dk/pweb", new HDTSpecification(), null);
            hdt.saveToHDT(f.getAbsolutePath(), null);
        } catch (ParserException e) {
            throw new IOException(e.getMessage());
        }

        File f1 = new File(f.getAbsolutePath() + ".index.v1-1");
        f1.deleteOnExit();
        f.deleteOnExit();
    }

    @Override
    public void addDataset(Dataset dataset) throws IOException {
        Iterator<Fragment> iterator = dataset.getFragments();
        while(iterator.hasNext()) {
            Fragment f = iterator.next();
            fragments.add(f);
            fragmentKeys.add(f.getPredicate());
        }

        try {
            updateDatastore();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        reloadDatastore();
    }

    @Override
    public void addFragment(Fragment fragment) throws IOException {
        fragments.add(fragment);
        fragmentKeys.add(fragment.getPredicate());

        try {
            updateDatastore();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void reloadDatastore() throws IOException {
        hdt = HDTManager.loadIndexedHDT(f.getAbsolutePath());
    }

    private void updateDatastore() throws Exception {
        TripleWriter writer = HDTManager.getHDTWriter(f.getAbsolutePath(), "http://qweb.cs.aau.dk/pweb/metadata", new HDTSpecification());

        for(Fragment f : fragments) {
            addFragment(f, writer);
        }

        writer.close();
    }

    private void addFragment(Fragment fragment, TripleWriter writer) throws IOException {
        for(Triple t : fragment.getTriples()) {
            writer.addTriple(new TripleString(t.getSubject(), t.getPredicate(), t.getObject()));
        }
    }

    @Override
    public QueryResult processQuery(String sparql) {
        QueryResult result;
        HDTGraph graph = new HDTGraph(hdt);
        Model model = ModelFactory.createModelForGraph(graph);

        Query query = QueryFactory.create(sparql);
        QueryExecution qe = QueryExecutionFactory.create(query, model);

        try {
            ResultSet results = qe.execSelect();
            result = new QueryResult(results.getResultVars());
            while (results.hasNext()) {
                QuerySolution soln = results.nextSolution();
                Iterator<String> varNameIterator = soln.varNames();
                Map<String, String> map = new HashMap<>();
                while (varNameIterator.hasNext()) {
                    String v = varNameIterator.next();
                    RDFNode val = soln.get(v);
                    map.put(v, val == null ? "(nil)" : escapeHtml4(val.toString()));
                }
                result.addResultRow(map);
            }
        } finally {
            qe.close();
        }

        return result;
    }
}
