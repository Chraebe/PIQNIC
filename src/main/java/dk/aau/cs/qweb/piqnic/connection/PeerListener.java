package dk.aau.cs.qweb.piqnic.connection;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.FragmentFactory;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.*;

public class PeerListener implements IPeerListener {
    private final int port;
    private Set<UUID> processed = new HashSet<>();
    public static int NUM_MSG = 0;

    public PeerListener(int port) {
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket connectionSocket = serverSocket.accept();
            PeerListenerThread thread = new PeerListenerThread(connectionSocket);
            thread.start();
        }
    }

    private class PeerListenerThread extends Thread {
        private final Socket connectionSocket;
        private PrintWriter writer;

        PeerListenerThread(Socket socket) {
            connectionSocket = socket;
        }

        @Override
        public void run() {
            InputStream input;
            OutputStream output;
            try {
                input = connectionSocket.getInputStream();
                output = connectionSocket.getOutputStream();
            } catch (IOException e) {
                return;
            }

            Scanner scanner = new Scanner(input, "UTF-8");
            try {
                writer = new PrintWriter(new OutputStreamWriter(output, "UTF-8"), true);
            } catch (UnsupportedEncodingException e) {
                return;
            }

            processConnection(scanner, writer);
            writer.close();
            scanner.close();
            try {
                input.close();
                output.close();
                connectionSocket.close();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void processConnection(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();

        switch (line) {
            case "0":
                join(scanner, writer);
                break;
            case "1":
                shuffle(scanner, writer);
                break;
            case "2":
                addFragment(scanner, writer);
                break;
            case "3":
                updateFragment(scanner, writer);
                break;
            case "4":
                processTriplePattern(scanner, writer);
                break;
            case "5":
                passJoin(scanner, writer);
                break;
            case "6":
                estimateCardinality(scanner, writer);
                break;
            case "7":
                addFragmentForTests(scanner, writer);
                break;
            case "8":
                processTriplePatternBound(scanner, writer);
                break;
            case "9":
                getConstituents(writer);
                break;
        }
    }

    public void getConstituents(PrintWriter writer) {
        PiqnicClient.nodeInstance.getConstituents(writer);
    }

    @Override
    public void join(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        String[] words = line.split(";");
        Peer p = new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2]));

        List<Peer> peers = PiqnicClient.nodeInstance.getNeighbours();
        for(Peer peer : peers) {
            writer.println(peer.getAddress() + ";" + peer.getPort() + ";" + peer.getId());
        }

        if(PiqnicClient.nodeInstance.getDatasets().size() == 0) {
            Peer peer = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
            try {
                peer.passJoin(p);
            } catch(IOException e) {}
        } else {
            int num = 0;
            for(Dataset d : PiqnicClient.nodeInstance.getDatasets()) {
                for(MetaFragmentBase f : d.getFragments()) {
                    if(num >= 5) return;
                    num++;
                    try {
                        p.addFragmentInit(f, PiqnicClient.nodeInstance.getTriples(f), 1);
                        f.addPeer(p);
                    } catch (IOException e) {}
                }
            }
        }
    }

    @Override
    public void passJoin(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        String[] words = line.split(";");
        Peer p = new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2]));

        if(PiqnicClient.nodeInstance.getDatasets().size() == 0) {
            Peer peer = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
            try {
                peer.passJoin(p);
            } catch(IOException e) {}
        } else {
            int num = 0;
            for(Dataset d : PiqnicClient.nodeInstance.getDatasets()) {
                for(MetaFragmentBase f : d.getFragments()) {
                    if(num >= 5) return;
                    num++;
                    try {
                        p.addFragmentInit(f, PiqnicClient.nodeInstance.getTriples(f), 1);
                        f.addPeer(p);
                    } catch (IOException e) {}
                }
            }
        }
    }

    @Override
    public void shuffle(Scanner scanner, PrintWriter writer) {
        List<Peer> peers = new ArrayList<>();
        String line;
        while ((line = scanner.nextLine()) != null) {
            String[] words = line.split(";");
            peers.add(new Peer(words[1], Integer.parseInt(words[2]), UUID.fromString(words[0])));
        }

        List<Peer> retPeers = PiqnicClient.nodeInstance.getLeastRelated(peers.size());
        PiqnicClient.nodeInstance.getNeighbours().removeAll(retPeers);

        for (Peer peer : peers) {
            PiqnicClient.nodeInstance.addNeighbour(peer);
        }

        for (Peer p : retPeers) {
            writer.println(p.getId().toString() + ";" + p.getAddress() + ";" + p.getPort());
        }
    }

    @Override
    public void addFragment(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        String[] words = line.split(";");
        Peer o = new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2]));

        line = scanner.nextLine();
        words = line.split(";;");

        FragmentBase fragment = FragmentFactory.createFragment(words[0], words[1], new File(getRandomFilename()), o);
        int ttl = Integer.parseInt(words[2]);

        List<TripleString> triples = new ArrayList<>();
        while ((line = scanner.nextLine()) != null) {
            String[] t = line.split(";;");
            triples.add(new TripleString(t[0], t[1], t[2]));
        }

        if (!PiqnicClient.nodeInstance.hasFragment(fragment)) {
            PiqnicClient.nodeInstance.addFragment(fragment, triples);
            writer.println(PiqnicClient.nodeInstance.getIp() + ";" + PiqnicClient.nodeInstance.getPort() + ";" + PiqnicClient.nodeInstance.getId());
            if (ttl > 1) {
                Peer p = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
                try {
                    p.addFragment(fragment, triples, (ttl - 1), writer);
                } catch (IOException e) {
                }
            }
        } else {
            Peer p = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
            try {
                p.addFragment(fragment, triples, (ttl), writer);
            } catch (IOException e) {
            }
        }
    }

    private String getRandomFilename() {
        byte[] array = new byte[7];
        new Random().nextBytes(array);

        return new String(array, Charset.forName("UTF-8")) + ".hdt";
    }

    @Override
    public void updateFragment(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        String[] words = line.split(";");
        FragmentBase fragment = PiqnicClient.nodeInstance.getFragment(words[0], words[1]);
        if(fragment == null) return;

        List<TripleString> triples = new ArrayList<>();
        String cmd = scanner.nextLine();
        while ((line = scanner.nextLine()) != null) {
            words = line.split(";;");
            triples.add(new TripleString(words[0], words[1], words[2]));
        }

        if(cmd.equals("0")) {
            PiqnicClient.nodeInstance.addTriplesToFragment(fragment, triples);
        } else if(cmd.equals("1")) {
            PiqnicClient.nodeInstance.removeTriplesFromFragments(fragment, triples);
        }
    }

    @Override
    public void processTriplePattern(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        if (line == null) {
            writer.close();
            return;
        }

        NUM_MSG = 0;

        String[] words = line.split(";");
        UUID reqId = UUID.fromString(words[0]);
        if (processed.contains(reqId)) {
            writer.close();
            return;
        }

        processed.add(reqId);
        Triple triple = new Triple(words[2], words[3], words[4]);
        int ttl = Integer.parseInt(words[1]);
        List<QueryProcessorThread> threads = new ArrayList<>();

        PiqnicClient.nodeInstance.processTriplePattern(triple, writer);

        if (ttl > 1) {
            for (IPeer peer : PiqnicClient.nodeInstance.getNeighbours()) {
                NUM_MSG++;
                QueryProcessorThread t = new QueryProcessorThread(triple, peer, ttl - 1, reqId, writer);
                t.start();
                threads.add(t);
            }
        }

        while (isRunning(threads)) ;
        System.out.println("NUM_MSG=" + NUM_MSG);
        writer.println(":" + NUM_MSG);
        writer.close();
    }

    @Override
    public void processTriplePatternBound(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        if (line == null) {
            writer.close();
            return;
        }

        NUM_MSG = 0;

        long start = System.currentTimeMillis();

        List<Map<String, String>> bindings = new ArrayList<>();
        List<String> bLines = new ArrayList<>();
        String bind;
        while ((bind = scanner.nextLine()) != null) {
            if (bind.equals("EOF")) break;

            Map<String, String> bs = new HashMap<>();
            String[] binds = bind.split(";;");
            for (int i = 0; i < binds.length; i++) {
                String b = binds[i];
                bs.put(b.substring(0, b.indexOf("=")), b.substring(b.indexOf("=") + 1));
            }
            bLines.add(bind);
            bindings.add(bs);
        }

        String[] words = line.split(";");
        UUID reqId = UUID.fromString(words[0]);
        if (processed.contains(reqId)) {
            writer.close();
            return;
        }

        processed.add(reqId);
        Triple triple = new Triple(words[2], words[3], words[4]);

        int ttl = Integer.parseInt(words[1]);
        List<QueryProcessorBoundThread> threads = new ArrayList<>();

        if (ttl > 1) {
            for (IPeer peer : PiqnicClient.nodeInstance.getNeighbours()) {
                NUM_MSG++;
                QueryProcessorBoundThread t = new QueryProcessorBoundThread(triple, peer, ttl - 1, bLines, reqId, writer);
                t.start();
                threads.add(t);
            }

        }

        PiqnicClient.nodeInstance.processTriplePatternBound(triple, bindings, writer);
        while (isRunningBound(threads) && ((System.currentTimeMillis() - start) < (500 * ttl))) ;
        System.out.println("NUM_MSG: " + NUM_MSG);
        writer.println(":" + NUM_MSG);
        writer.close();
    }

    @Override
    public void estimateCardinality(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        if (line == null) {
            writer.println(0);
            return;
        }

        String[] words = line.split(";");
        UUID reqId = UUID.fromString(words[0]);
        if (processed.contains(reqId)) {
            writer.println(0);
            return;
        }

        processed.add(reqId);
        Triple triple = new Triple(words[2], words[3], words[4]);
        int ttl = Integer.parseInt(words[1]);
        long est = PiqnicClient.nodeInstance.estimateResult(triple) * PiqnicClient.nodeInstance.getNeighbours().size() * ttl;

        writer.println(est);
    }

    private void addFragmentForTests(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        String[] words = line.split(";");
        Peer o = new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2]));

        line = scanner.nextLine();
        if (line == null) return;

        words = line.split(";");
        int ttl = Integer.parseInt(words[0]);
        FragmentBase fragment = FragmentFactory.createFragment(words[1], words[2], new File(words[3]), o);
        System.out.println("Adding fragment " + fragment.getBaseUri() + "/" + fragment.getId());
        if (PiqnicClient.nodeInstance.insertFragment(fragment)) {
            ttl = ttl - 1;
            writer.println(PiqnicClient.nodeInstance.getId() + ";" + PiqnicClient.nodeInstance.getIp() + ";" + PiqnicClient.nodeInstance.getPort());
        }

        if (ttl > 1) {
            IPeer peer = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
            try {
                peer.addFragmentForTest(fragment, ttl, writer);
            } catch (IOException e) {
            }
        }
    }

    private boolean isRunning(List<QueryProcessorThread> threads) {
        for (QueryProcessorThread t : threads) {
            if (t.isRunning) return true;
        }
        return false;
    }

    private boolean isRunningBound(List<QueryProcessorBoundThread> threads) {
        for (QueryProcessorBoundThread t : threads) {
            if (t.isRunning) return true;
        }
        return false;
    }

    private class QueryProcessorThread extends Thread {
        private final Triple triple;
        private final IPeer peer;
        private final int ttl;
        private final UUID reqId;
        private final PrintWriter writer;
        private boolean isRunning = false;

        QueryProcessorThread(Triple triple, IPeer peer, int ttl, UUID reqId, PrintWriter writer) {
            this.triple = triple;
            this.peer = peer;
            this.ttl = ttl;
            this.reqId = reqId;
            this.writer = writer;
        }

        boolean isRunning() {
            return isRunning;
        }

        @Override
        public void run() {
            isRunning = true;

            //System.out.println("Start " + peer.getPort());
            try {
                peer.processTriplePattern(reqId, ttl, triple, writer);
            } catch (IOException e) {
            }

            //System.out.println("End " + peer.getPort());
            isRunning = false;
        }
    }

    private class QueryProcessorBoundThread extends Thread {
        private final Triple triple;
        private final IPeer peer;
        private final int ttl;
        private List<String> bindings;
        private final UUID reqId;
        private final PrintWriter writer;
        private boolean isRunning = false;

        QueryProcessorBoundThread(Triple triple, IPeer peer, int ttl, List<String> bindings, UUID reqId, PrintWriter writer) {
            this.triple = triple;
            this.peer = peer;
            this.ttl = ttl;
            this.reqId = reqId;
            this.writer = writer;
            this.bindings = bindings;
        }

        boolean isRunning() {
            return isRunning;
        }

        @Override
        public void run() {
            isRunning = true;

            //System.out.println("Start " + peer.getPort());
            try {
                peer.processTriplePatternBound(reqId, ttl, triple, bindings, writer);
            } catch (IOException e) {
            }

            //System.out.println("End " + peer.getPort());
            isRunning = false;
        }
    }
}
