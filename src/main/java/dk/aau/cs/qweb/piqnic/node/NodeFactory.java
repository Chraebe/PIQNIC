package dk.aau.cs.qweb.piqnic.node;

import java.io.IOException;
import java.util.UUID;

public class NodeFactory {
    public static INode createPiqnicNode(String ip, int port) throws IOException {
        return new PiqnicNode(ip, port);
    }

    public static INode createPiqnicNode(String ip, int port, UUID id) throws IOException {
        return new PiqnicNode(ip, port, id);
    }
}
