package dk.aau.cs.qweb.piqnic.jena.down;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicJenaFloodIterator;
import dk.aau.cs.qweb.piqnic.node.NodeBase;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.util.iterator.NiceIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class PiqnicJenaDownIterator extends NiceIterator<Triple> {
    private BufferedReader reader;
    private String nextLine;
    private boolean empty = false;
    private Triple origin;
    public static int NTB = 0;
    private PiqnicDownTripleBindings bindings;
    private boolean done = false;

    public PiqnicJenaDownIterator(BufferedReader reader, Triple triple) {
        this.origin = triple;
        this.reader = reader;
        bindings = new PiqnicDownTripleBindings(triple);
    }

    private PiqnicJenaDownIterator() {
        bindings = new PiqnicDownTripleBindings(new Triple(Node.ANY, Node.ANY, Node.ANY));
        empty = true;
    }

    public static PiqnicJenaDownIterator emptyIterator() {
        return new PiqnicJenaDownIterator();
    }

    public PiqnicDownTripleBindings getBindings() {
        while(hasNext()) next();
        bindings.removeDuplicates();
        return bindings;
    }

    private String bufferNext() {
        try {
            nextLine = reader.readLine();
            if(nextLine != null && nextLine.charAt(0) == ':')
                return bufferNext();
            return nextLine;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean hasNext() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if(empty) return false;
        boolean hasNext = nextLine != null || bufferNext() != null;

        if (!hasNext) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new IllegalStateException("I/O error when closing stream.", e);
            }
        }

        return hasNext;
    }

    @Override
    public Triple next() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!hasNext())
            throw new NoSuchElementException();

        if(PiqnicClient.test) {
            PiqnicJenaConstants.NM++;
            PiqnicJenaConstants.NTB += nextLine.getBytes().length;
        }
        String[] str = nextLine.split(";;");

        Triple t;
        try {
            t = new Triple(
                    getNode(str[0]),
                    getNode(str[1]),
                    getNode(str[2]));
        } catch (ArrayIndexOutOfBoundsException e) {
            //System.out.println("ArrayIndexOutOfBound: " + nextLine);
            throw new NoSuchElementException();
        }
        bindings.put(t);
        nextLine = null;

        return t;
    }

    private Node getNode(String element) {
        if(element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (firstChar == '"') {
            String noq = element.replace("\"", "");
            if( noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        } else {
            return NodeFactory.createURI(element);
        }
    }
}
