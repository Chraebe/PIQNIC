package dk.aau.cs.qweb.piqnic.jena.graph;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.bind.PiqnicBindings;
import dk.aau.cs.qweb.piqnic.jena.bind.PiqnicJenaBindIterator;
import dk.aau.cs.qweb.piqnic.jena.down.PiqnicJenaDownIterator;
import dk.aau.cs.qweb.piqnic.jena.solver.*;
import dk.aau.cs.qweb.piqnic.jena.solver.cache.PiqnicCache;
import dk.aau.cs.qweb.piqnic.test.TestConstants;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class PiqnicGraph extends GraphBase {
    private static final Logger log = LoggerFactory.getLogger(PiqnicGraph.class);
    private PiqnicStatistics statistics = new PiqnicStatistics(this);
    private static PiqnicCapabilities capabilities = new PiqnicCapabilities();
    private ReorderTransformation reorderTransform;
    public static PiqnicCache cache = new PiqnicCache();

    static {
        QC.setFactory(ARQ.getContext(), OpExecutorPiqnic.opExecFactoryPiqnic);
        PiqnicEngine.register();
    }

    public PiqnicGraph() {
        reorderTransform = new ReorderTransformationPiqnic(this);
        cache.destroy();
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple jenaTriple) {
        if (cache.isCached(jenaTriple)) {
            return cache.get(jenaTriple);
        }

        if(PiqnicJenaConstants.PROCESSOR == PiqnicJenaConstants.ProcessingType.FLOOD)
            return flood(jenaTriple);
        return down(jenaTriple);
    }

    public ExtendedIterator<Pair<Triple, Binding>> graphBaseFindBind(Triple jenaTriple, PiqnicBindings bindings) {
        Socket socket;
        PrintWriter out;
        BufferedReader in;
        try {
            socket = new Socket(PiqnicClient.nodeInstance.getIp(), PiqnicClient.nodeInstance.getPort());
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException | NullPointerException e) {
            return PiqnicJenaBindIterator.emptyIterator();
        }

        out.println(8);
        out.println(UUID.randomUUID() + ";" + Configuration.instance.getTimeToLive() + ";"
                + jenaTriple.getSubject().toString() + ";"
                + jenaTriple.getPredicate().toString() + ";"
                + jenaTriple.getObject().toString());

        int size = bindings.size();
        for(int i = 0; i < size; i++) {
            Binding binding = bindings.get(i);
            Iterator<Var> it = binding.vars();
            String str = "";

            while(it.hasNext()) {
                Var v = it.next();
                String var = v.asNode().toString();
                String b = binding.get(v).toString();
                str = str.concat(var + "=" + b + ";;");
            }
            if(str.length() > 2)
                str = str.substring(0, str.length()-2);

            if(str.length() > 1)
                out.println(str);
        }
        out.println("EOF");
        PiqnicJenaConstants.NM++;

        return new PiqnicJenaBindIterator(in);
    }

    private NiceIterator<Triple> flood(Triple jenaTriple) {
        Socket socket;
        PrintWriter out;
        BufferedReader in;
        try {
            socket = new Socket(PiqnicClient.nodeInstance.getIp(), PiqnicClient.nodeInstance.getPort());
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException | NullPointerException e) {
            return PiqnicJenaFloodIterator.emptyIterator();
        }

        out.println(4);
        out.println(UUID.randomUUID() + ";" + Configuration.instance.getTimeToLive() + ";"
                + jenaTriple.getSubject().toString() + ";"
                + jenaTriple.getPredicate().toString() + ";"
                + jenaTriple.getObject().toString());
        out.println("");
        PiqnicJenaConstants.NM++;

        return new PiqnicJenaFloodIterator(in, jenaTriple);
    }

    private NiceIterator<Triple> bind(Triple jenaTriple) {
        return PiqnicJenaFloodIterator.emptyIterator();
    }

    private NiceIterator<Triple> down(Triple jenaTriple) {
        Socket socket;
        PrintWriter out;
        BufferedReader in;
        try {
            socket = new Socket(PiqnicClient.nodeInstance.getIp(), PiqnicClient.nodeInstance.getPort());
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException | NullPointerException e) {
            return PiqnicJenaDownIterator.emptyIterator();
        }

        out.println(4);
        out.println(UUID.randomUUID() + ";" + Configuration.instance.getTimeToLive() + ";"
                + jenaTriple.getSubject().toString() + ";"
                + jenaTriple.getPredicate().toString() + ";"
                + jenaTriple.getObject().toString());
        out.println("");

        return new PiqnicJenaDownIterator(in, jenaTriple);
    }


    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        return statistics;
    }

    @Override
    public Capabilities getCapabilities() {
        return capabilities;
    }

    @Override
    protected int graphBaseSize() {
        //return (int)statistics.getStatistic(Node.ANY, Node.ANY, Node.ANY);
        return 1000000000;
    }

    public ReorderTransformation getReorderTransform() {
        return reorderTransform;
    }

    @Override
    public void close() {
        super.close();
    }
}