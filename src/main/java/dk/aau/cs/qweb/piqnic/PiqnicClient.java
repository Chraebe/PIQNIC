package dk.aau.cs.qweb.piqnic;

import dk.aau.cs.qweb.piqnic.client.ClientFactory;
import dk.aau.cs.qweb.piqnic.client.IClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.connection.IPeerListener;
import dk.aau.cs.qweb.piqnic.connection.PeerListenerFactory;
import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.FragmentFactory;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.network.NetworkManager;
import dk.aau.cs.qweb.piqnic.node.INode;
import dk.aau.cs.qweb.piqnic.node.NodeFactory;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.test.TestConstants;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class PiqnicClient {
    public static INode nodeInstance;
    public static boolean running = true;
    public static NetworkManager manager = new NetworkManager();
    public static boolean test = false;
    public static Map<String, String> fileMap = new HashMap<>();

    private static List<Triple> getRandomClients(List<Triple> list) {
        List<Triple> ret = new ArrayList<>();
        if (list.size() == 0) return ret;

        Random rand = new Random();
        for (int i = 0; i < Configuration.instance.getNeighbours(); i++) {
            Triple t = list.get(rand.nextInt(list.size() - 1));
            if (UUID.fromString(t.getObject()).equals(nodeInstance.getId()) || ret.contains(t)) {
                i--;
                continue;
            }
            ret.add(t);
        }

        return ret;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
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

        int current = 0;
        if (args.length > 1) {
            test = true;
            int num = Integer.parseInt(args[1]);
            current = num;
            Configuration.instance.setListenerPort(30000 + num);
            Configuration.instance.setCliPort(31000 + num);
            Configuration.instance.setTestPort(32000 + num);
            nodeInstance = NodeFactory.createPiqnicNode(ip, Configuration.instance.getListenerPort(), UUID.fromString("e5381de8-04d3-4e42-b33b-031208ecf" + ((num < 10) ? "00" + num : ((num < 100) ? "0" + num : num))));
            TestConstants.NODE_NUM = num;
        } else
            nodeInstance = NodeFactory.createPiqnicNode(ip, Configuration.instance.getListenerPort());

        // java -jar [filename.jar] config.json 0 200 10 predicates hdt/clients
        Runnable runnableListener =
                () -> {
                    IPeerListener listener = PeerListenerFactory.createPeerListener(nodeInstance.getPort());
                    try {
                        System.out.println("Listener starting...");
                        listener.start();
                    } catch (IOException e) {
                        System.err.println("Error, could not start listener " + e.getMessage());
                    }
                };

        Thread listenerThread = new Thread(runnableListener);
        listenerThread.start();

        runClient();

        if (test) {
            int num = Integer.parseInt(args[2]);
            TestConstants.NUM_CLIENTS = num;
            TestConstants.REPLICATION = Integer.parseInt(args[3]);
            TestConstants.NUM_NEIGHBOURS = Integer.parseInt(args[4]);
            Configuration.instance.setNeighbours(TestConstants.NUM_NEIGHBOURS);
            List<Triple> clients = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                clients.add(new Triple("" + (30000 + i), "" + (31000 + i), UUID.fromString("e5381de8-04d3-4e42-b33b-031208ecf" + ((i < 10) ? "00" + i : ((i < 100) ? "0" + i : i))).toString()));
            }

            List<Triple> cs = getRandomClients(clients);
            for (Triple t : cs) {
                Peer peer = new Peer(nodeInstance.getIp(), Integer.parseInt(t.getSubject()), UUID.fromString(t.getObject()));
                nodeInstance.addNeighbour(peer);
            }

            Map<String, Integer> depMap = new HashMap<>();

            String base = "/q/storage/Aebeloe/datasets";

            // Load fragments
            BufferedReader reader = new BufferedReader(new FileReader(base + "/dataset_distro"));
            IPeer peer = new Peer((PiqnicNode)nodeInstance);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split(";");
                int node = Integer.parseInt(words[1]);
                if(node != current) continue;
                String baseUri = words[0];
                Dataset dataset = new Dataset(baseUri, peer);

                String fFile = base + "/" + baseUri + "/fragments";

                BufferedReader in = new BufferedReader(new FileReader(fFile));
                String ln;
                while ((ln = in.readLine()) != null) {
                    String[] ws = ln.split(";");

                    MetaFragmentBase fragment = FragmentFactory.createMetaFragment(baseUri, ws[0], new File(base + "/" + baseUri + "/" + ws[1] + ".hdt"), new Peer((PiqnicNode)nodeInstance), new HashSet<>());
                    fragment.addPeers(addFragment(fragment, TestConstants.REPLICATION));
                    dataset.addFragment(fragment);
                }
                in.close();

                nodeInstance.addDataset(dataset);
            }
            reader.close();
            System.out.println("Ready");

            /*List<File> files = listFilesForFolder(new File(args[5] + "/" + args[2] + "_" + args[3] + "/client" + args[1] + "/fragments"));
            Gson gson = new Gson();
            for(File f : files) {
                String str = readFile(f.getAbsolutePath(), Charset.defaultCharset());
                FragmentBase fragmentBase = gson.fromJson(str, FragmentBase.class);

                reader = new BufferedReader(new FileReader(depMap.get(fragmentBase.getPredicate())));
                String ln = reader.readLine();
                while (ln != null) {
                    fragmentBase.addDependent(ln);
                    ln = reader.readLine();
                }
                reader.close();

                nodeInstance.addFragmentBase(fragmentBase);
            }*/
        }
        //if (test) PiqnicClient.manager.start();
    }

    private static List<IPeer> addFragment(FragmentBase fragmentBase, int ttl) throws IOException {
        Socket socket;
        try {
            socket = new Socket(nodeInstance.getIp(), nodeInstance.getPort());
        } catch (Exception e) {
            return new ArrayList<>();
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out.println(7);
        out.println(fragmentBase.getOwner().getAddress() + ";" + fragmentBase.getOwner().getPort() + ";" + fragmentBase.getOwner().getId());
        out.println(ttl + ";" + fragmentBase.getBaseUri() + ";" + fragmentBase.getId() + ";" + fragmentBase.getFile().getAbsolutePath());

        List<IPeer> ret = new ArrayList<>();
        String line;
        while((line = reader.readLine()) != null) {
            String[] word = line.split(";");
            ret.add(new Peer(word[1], Integer.parseInt(word[2]), UUID.fromString(word[0])));
        }
        return ret;
    }

    private static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static List<File> listFilesForFolder(final File folder) {
        List<File> ret = new ArrayList<>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                ret.addAll(listFilesForFolder(fileEntry));
            } else {
                ret.add(fileEntry);
            }
        }
        return ret;
    }

    private static void runClient() {
        Runnable runnableClient =
                () -> {
                    while (running) {
                        IClient client = ClientFactory.createClient(Configuration.instance.getCliPort());
                        try {
                            System.out.println("CLI starting...");
                            client.start();
                        } catch (IOException e) {
                            System.err.println("Error, could not start client " + e.getMessage());
                            return;
                        }
                    }
                };

        Runnable runnableTestClient =
                () -> {
                    while (running) {
                        IClient client = ClientFactory.createTestClient(Configuration.instance.getTestPort());
                        try {
                            System.out.println("Test Client starting...");
                            client.start();
                        } catch (IOException e) {
                            System.err.println("Error, could not start client " + e.getMessage());
                            return;
                        }
                    }
                };

        Thread clientThread = new Thread(runnableClient);
        Thread clientTestThread = new Thread(runnableTestClient);
        try {
            clientThread.start();
            if (test) clientTestThread.start();
        } catch (NoSuchElementException e) {
            runClient();
        }
    }
}