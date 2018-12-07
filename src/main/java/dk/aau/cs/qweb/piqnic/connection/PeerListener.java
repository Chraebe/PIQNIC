package dk.aau.cs.qweb.piqnic.connection;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.FragmentFactory;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class PeerListener implements IPeerListener {
    private final int port;
    private Set<UUID> processed = new HashSet<>();

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

        /*
         * 0 Join (IP, Port, Id) -> Neighbours
         * 1 Shuffle (Peers) -> Peers
         * 2 AddFragment (Replicas, Nodes, FragmentBase, Stream(Triples)) -> Nodes
         * 3 UpdateFragment (FragmentBase, Stream(Triples))
         * 4 ProcessTriplePattern (TriplePattern, TTL, IDs) -> Stream(Triples)
         * 5 FragmentDependence (Stream(URI, FragmentID)) -> Boolean
         * 6 EstimateCardinality (TriplePattern, TTL, IDs) -> Estimation
         * 8 ProcessTriplePatternBound (TriplePattern,TTL,Bindings)
         */

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
                fragmentDependence(scanner, writer);
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
        }
    }

    @Override
    public void join(Scanner scanner, PrintWriter writer) {
        //Todo
        throw new NotImplementedException();
    }

    @Override
    public void shuffle(Scanner scanner, PrintWriter writer) {
        List<Peer> peers = new ArrayList<>();
        String line;
        while ((line = scanner.nextLine()) != null) {
            String[] words = line.split(";");
            peers.add(new Peer(words[1], Integer.parseInt(words[2]), UUID.fromString(words[0])));
        }

        //FIXME NOT random nodes, the num least related nodes
        List<IPeer> retPeers = PiqnicClient.nodeInstance.getRandomPeers(peers.size());
        PiqnicClient.nodeInstance.getNeighbours().removeAll(retPeers);

        for (IPeer peer : peers) {
            PiqnicClient.nodeInstance.addNeighbour(peer);
        }

        for (IPeer p : retPeers) {
            writer.println(p.getId().toString() + ";" + p.getAddress() + ";" + p.getPort());
        }
    }

    @Override
    public void addFragment(Scanner scanner, PrintWriter writer) {
        //Todo
        throw new NotImplementedException();
    }

    @Override
    public void updateFragment(Scanner scanner, PrintWriter writer) {
        //Todo
        throw new NotImplementedException();
    }

    @Override
    public void processTriplePattern(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        if (line == null) {
            writer.close();
            return;
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
        List<QueryProcessorThread> threads = new ArrayList<>();

        PiqnicClient.nodeInstance.processTriplePattern(triple, writer);

        if (ttl > 1) {
            for (IPeer peer : PiqnicClient.nodeInstance.getNeighbours()) {
                QueryProcessorThread t = new QueryProcessorThread(triple, peer, ttl - 1, reqId, writer);
                t.start();
                threads.add(t);
            }
        }


        while (isRunning(threads)) ;
        writer.close();
    }

    @Override
    public void processTriplePatternBound(Scanner scanner, PrintWriter writer) {
        String line = scanner.nextLine();
        if (line == null) {
            writer.close();
            return;
        }

        List<Map<String, String>> bindings = new ArrayList<>();
        List<String> bLines = new ArrayList<>();
        String bind;
        while((bind = scanner.nextLine()) != null) {
            if(bind.equals("EOF")) break;

            Map<String, String> bs = new HashMap<>();
            String[] binds = bind.split(";;");
            for(int i = 0; i < binds.length; i++) {
                String b = binds[i];
                bs.put(b.substring(0, b.indexOf("=")), b.substring(b.indexOf("=")+1));
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
                QueryProcessorBoundThread t = new QueryProcessorBoundThread(triple, peer, ttl - 1, bLines, reqId, writer);
                t.start();
                threads.add(t);
            }

        }

        PiqnicClient.nodeInstance.processTriplePatternBound(triple, bindings, writer);
        while (isRunningBound(threads)) ;
        System.out.println("Done");
        writer.close();
    }

    @Override
    public void fragmentDependence(Scanner scanner, PrintWriter writer) {
        // Todo
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
        if (line == null) return;

        String[] words = line.split(";");
        int ttl = Integer.parseInt(words[0]);
        FragmentBase fragment = FragmentFactory.createFragment(words[1], words[2], new File(words[3]));
        System.out.println("Adding fragment " + fragment.getBaseUri() + "/" + fragment.getId());
        if(PiqnicClient.nodeInstance.insertFragment(fragment)) {
            ttl = ttl-1;
            writer.println(PiqnicClient.nodeInstance.getId() + ";" + PiqnicClient.nodeInstance.getIp() + ";" + PiqnicClient.nodeInstance.getPort());
        }

        if(ttl > 1) {
            IPeer peer = PiqnicClient.nodeInstance.getRandomPeers(1).get(0);
            try {
                peer.addFragmentForTest(fragment, ttl, writer);
            } catch (IOException e) {}
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
