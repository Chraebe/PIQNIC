package dk.aau.cs.qweb.piqnic.jena.solver;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.util.iterator.NiceIterator;
import pl.edu.icm.jlargearrays.LargeArray;
import pl.edu.icm.jlargearrays.ObjectLargeArray;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class PiqnicJenaFloodIterator extends NiceIterator<Triple> {
    private BufferedReader reader;
    private String nextLine;
    private boolean empty = false;
    private Triple origin;
    private Set<String> seen = new HashSet<>();

    public PiqnicJenaFloodIterator(BufferedReader reader, Triple triple) {
        this.origin = triple;
        this.reader = reader;
    }
    private PiqnicJenaFloodIterator() {
        empty = true;
    }

    public static PiqnicJenaFloodIterator emptyIterator() {
        return new PiqnicJenaFloodIterator();
    }

    private String bufferNext() {
        try {
            nextLine = reader.readLine();
            if(nextLine != null && nextLine.startsWith(":")) {
                PiqnicJenaConstants.NM+=Integer.parseInt(nextLine.substring(1));
                return bufferNext();
            }
            if((nextLine != null && nextLine.charAt(0) == ':') || seen.contains(nextLine))
                return bufferNext();

            seen.add(nextLine);
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
