package dk.aau.cs.qweb.piqnic.jena.solver;

import dk.aau.cs.qweb.piqnic.jena.graph.PiqnicGraph;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.optimizer.Pattern;
import org.apache.jena.sparql.engine.optimizer.StatsMatcher;
import org.apache.jena.sparql.engine.optimizer.reorder.PatternTriple;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformationSubstitution;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.sparql.sse.Item;

import static org.apache.jena.sparql.engine.optimizer.reorder.PatternElements.TERM;
import static org.apache.jena.sparql.engine.optimizer.reorder.PatternElements.VAR;

public class ReorderTransformationPiqnic extends ReorderTransformationSubstitution {
    /** Maximum value for a match involving two terms. */
    public static final int MultiTermMax = 50;
    /** The number of triples used for the base scale */
    public static final int MultiTermSampleSize = 10000 ;

    private final GraphStatisticsHandler stats;
    public final StatsMatcher matcher = new StatsMatcher() ;
    final long numTriples ;		// Actual number of triples of the dataset.

    public ReorderTransformationPiqnic(PiqnicGraph graph) {
        stats = graph.getStatisticsHandler();
        numTriples = graph.size();
        initializeMatcher();
    }

    private void initializeMatcher () {
        Item type = Item.createNode(NodeConst.nodeRDFType);

        //matcher.addPattern(new Pattern(1,   TERM, TERM, TERM)) ;     // SPO - built-in - not needed as a rule

        // Numbers chosen as an approximation for a graph of 10K triples
        matcher.addPattern(new Pattern(5,	TERM, TERM, VAR)) ;     // SP?
        matcher.addPattern(new Pattern(1000,VAR, type, TERM)) ;    // ? type O -- worse than ?PO
        matcher.addPattern(new Pattern(5,	VAR,  TERM, TERM)) ;    // ?PO
        matcher.addPattern(new Pattern(5,	TERM, VAR, TERM)) ;    // S?O

        matcher.addPattern(new Pattern(40,	TERM, VAR,  VAR)) ;     // S??
        matcher.addPattern(new Pattern(200,	VAR,  VAR,  TERM)) ;    // ??O
        matcher.addPattern(new Pattern(2000,VAR,  TERM, VAR)) ;     // ?P?

        matcher.addPattern(new Pattern(MultiTermSampleSize, VAR,  VAR,  VAR)) ;     // ???
    }

    @Override
    protected double weight(PatternTriple pt)
    {
        //System.out.println("ReorderTransformation: "+pt.toString());
        if(pt.subject.isNode() && pt.predicate.isNode() && pt.object.isNode()) {
            long x = stats.getStatistic(pt.subject.getNode(), pt.predicate.getNode(), pt.object.getNode());
            //System.out.println(x);
            return x;
        }

        double x = matcher.match(pt);

        if ( x < MultiTermMax )
        {
            //System.out.println(x);
            return x;
        }
        long S = stats.getStatistic(pt.subject.getNode(), Node.ANY, Node.ANY);
        long P = stats.getStatistic(Node.ANY, pt.predicate.getNode(), Node.ANY);
        long O = stats.getStatistic(Node.ANY, Node.ANY, pt.object.getNode());
        //System.out.println(S + " " + P + " " + O);

        if ( S == 0 || P == 0 || O == 0 ) {
            return 0 ;
        }

        x = -1 ;
        if ( S > 0 ) x = S ;
        if ( P > 0 && P < x ) x = P ;
        if ( O > 0 && O < x ) x = O ;

        //System.out.println(x);
        return x;
    }
}
