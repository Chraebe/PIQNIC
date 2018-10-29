package dk.aau.cs.qweb.pweb;

import dk.aau.cs.qweb.pweb.cli.ClientFactory;
import dk.aau.cs.qweb.pweb.cli.IClient;
import dk.aau.cs.qweb.pweb.config.Configuration;
import dk.aau.cs.qweb.pweb.connection.IPeerListener;
import dk.aau.cs.qweb.pweb.connection.PeerListenerFactory;
import dk.aau.cs.qweb.pweb.node.INode;
import dk.aau.cs.qweb.pweb.node.NodeFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class PWebClient {
    public static INode nodeInstance;
    public static boolean running = true;

    public static void main(String[] args) throws IOException {
        if(args.length == 0) {
            System.out.println("Usage: java -jar [filename].jar [config.json]");
            return;
        }

        try {
            Configuration.instance = new Configuration(args[0]);
        } catch (IOException e) {
            System.out.println("Usage: java -jar [filename].jar [config.json]");
            return;
        }

        String ip;
        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        }

        nodeInstance = NodeFactory.createPWebNode(ip, Configuration.instance.getListenerPort());

        Runnable runnableListener =
                () -> {
                    IPeerListener listener = PeerListenerFactory.createPeerListener(nodeInstance.getPort());
                    try {
                        System.out.println("Listener starting...");
                        listener.start();
                    } catch (IOException e) {
                        System.err.println("Error, could not start listener");
                    }
                };

        Thread listenerThread = new Thread(runnableListener);
        listenerThread.start();

        Runnable runnableClient =
                () -> {
                    while (running) {
                        IClient client = ClientFactory.createClient(Configuration.instance.getCliPort());
                        try {
                            System.out.println("Client starting...");
                            client.start();
                        } catch (IOException e) {
                            System.err.println("Error, could not start client");
                        }
                    }
                };

        Thread clientThread = new Thread(runnableClient);
        clientThread.start();
    }
}
// PICNIC