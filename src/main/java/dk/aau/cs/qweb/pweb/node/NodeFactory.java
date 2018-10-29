package dk.aau.cs.qweb.pweb.node;

import java.io.IOException;

public class NodeFactory {
    public static INode createPWebNode(String ip, int port) throws IOException {
        return new PWebNode(ip, port);
    }
}
