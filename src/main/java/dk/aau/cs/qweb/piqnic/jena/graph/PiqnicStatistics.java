package dk.aau.cs.qweb.piqnic.jena.graph;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PiqnicStatistics implements GraphStatisticsHandler {
    private final PiqnicGraph graph;

    PiqnicStatistics(PiqnicGraph graph) {
        this.graph = graph;
    }

    @Override
    public long getStatistic(Node subject, Node predicate, Node object) {
        if ((subject.isVariable() || subject.equals(Node.ANY))
                && (predicate.isVariable() || predicate.equals(Node.ANY))
                && (object.isVariable() || object.equals(Node.ANY)))
            return graph.graphBaseSize();
        try {
            Socket socket = new Socket(PiqnicClient.nodeInstance.getIp(), PiqnicClient.nodeInstance.getPort());
            List<UUID> ids = new ArrayList<>();
            ids.add(PiqnicClient.nodeInstance.getId());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(6);
            out.println(UUID.randomUUID() + ";" + Configuration.instance.getTimeToLive() + ";"
                    + subject.toString() + ";"
                    + predicate.toString() + ";"
                    + object.toString());
            return Long.parseLong(in.readLine());
        } catch (Exception e) {
            // Something went wrong. Worst case estimation instead of crashing.
            return Long.MAX_VALUE;
        }
    }
}
