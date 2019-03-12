package dk.aau.cs.qweb.piqnic.jena;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import java.util.NoSuchElementException;

public class PiqnicJenaConstants {
    private static final String BASE_URI = "http://qweb.cs.aau.dk/piqnic/";
    public static final Resource PIQNIC_GRAPH = ResourceFactory.createResource(BASE_URI+"fuseki#PIQNICGraph") ;
    public static long NTB = 0;
    public static int NM = 0;

    public final static int BIND_NUM = 1000;
    public static ProcessingType PROCESSOR = ProcessingType.BIND;

    public enum ProcessingType {
        FLOOD, BIND, DOWN;

        public static ProcessingType fromString(String str) {
            if(str.equals("flood") || str.equals("FLOOD"))
                return FLOOD;
            if(str.equals("bind") || str.equals("BIND"))
                return BIND;
            if(str.equals("down") || str.equals("DOWN"))
                return DOWN;
            throw new NoSuchElementException();
        }
    }
}
