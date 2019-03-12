package dk.aau.cs.qweb.piqnic.peer;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.connection.PeerListener;
import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.FragmentFactory;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.util.*;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.*;

public class Peer implements IPeer, Comparable<Peer> {
    private String address;
    private int port;
    private UUID id;
    private Constituents constituents = new Constituents();

    public Peer(String address, int port, UUID id) {
        this.address = address;
        this.port = port;
        this.id = id;
    }

    public Peer(PiqnicNode node) {
        this.address = node.getIp();
        this.port = node.getPort();
        this.id = node.getId();
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public void processTriplePattern(UUID id, int ttl, Triple triple, PrintWriter writer) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }
        //System.out.println("Passing " + triple.toString() + " on to " + port + " with ttl " + ttl);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(4);
        out.println(id.toString() + ";" + ttl + ";" + triple.getSubject() + ";" + triple.getPredicate() + ";" + triple.getObject());

        String line = null;
        while ((line = reader.readLine()) != null) {
            if(line.startsWith(":")) {
                PeerListener.NUM_MSG += Integer.parseInt(line.substring(1));
                continue;
            }
            writer.println(line);
        }

        socket.close();
    }

    @Override
    public void processTriplePatternBound(UUID id, int ttl, Triple triple, List<String> bindings, PrintWriter writer) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }
        //System.out.println("Passing " + triple.toString() + " on to " + port + " with ttl " + ttl);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(8);
        out.println(id.toString() + ";" + ttl + ";" + triple.getSubject() + ";" + triple.getPredicate() + ";" + triple.getObject());
        for(String b : bindings) {
            out.println(b);
        }
        out.println("EOF");

        String line = null;
        while ((line = reader.readLine()) != null) {
            if(line.startsWith(":")) {
                PeerListener.NUM_MSG += Integer.parseInt(line.substring(1));
                continue;
            }
            writer.println(line);
        }

        socket.close();
    }

    @Override
    public List<Peer> shuffle(List<Peer> peers) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return new ArrayList<>();
        }
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(1);
        for (Peer peer : peers) {
            out.println(peer.getId().toString() + ";" + peer.getAddress() + ";" + peer.getPort());
        }
        out.close();

        List<Peer> ret = new ArrayList<>();
        String response;
        while ((response = in.readLine()) != null) {
            String[] words = response.split(";");
            ret.add(new Peer(words[1], Integer.parseInt(words[2]), UUID.fromString(words[0])));
        }

        socket.close();
        return ret;
    }

    @Override
    public long estimateNumResults(UUID id, int ttl, Triple triple) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return 0;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(6);
        out.println(id.toString() + ";" + ttl + ";" + triple.getSubject() + ";" + triple.getPredicate() + ";" + triple.getObject());
        String line = reader.readLine();


        long res;
        if(line == null) {
            res = 0;
        } else {
            res = Long.parseLong(line);
        }

        socket.close();
        return res;
    }

    @Override
    public void addFragmentForTest(FragmentBase fragment, int ttl, PrintWriter writer) throws IOException {
        Socket socket = new Socket(address, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(7);
        out.println(ttl + ";" + fragment.getBaseUri() + ";" + fragment.getId() + ";" + fragment.getFile().getAbsolutePath());

        String response;
        while ((response = in.readLine()) != null) {
            writer.println(response);
        }

        socket.close();
    }

    @Override
    public void addFragment(FragmentBase fragment, List<TripleString> triples, int ttl, PrintWriter writer) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(2);
        out.println(fragment.getOwner().getAddress() + ";" + fragment.getOwner().getPort() + ";" + fragment.getOwner().getId());
        out.println(fragment.getBaseUri() + ";;" + fragment.getId() + ";;" + ttl);

        for(TripleString t : triples) {
            out.println(t.getSubject() + ";;" + t.getPredicate() + ";;" + t.getObject());
        }

        String response;
        while ((response = in.readLine()) != null) {
            writer.println(response);
        }

        socket.close();
    }

    @Override
    public void passJoin(Peer p) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(5);
        out.println(p.getAddress() + ";" + p.getPort() + ";" + p.getId());
    }

    @Override
    public void join(Peer p) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(0);
        out.println(p.getAddress() + ";" + p.getPort() + ";" + p.getId());

        String response;
        while ((response = in.readLine()) != null) {
            String[] words = response.split(";");
            PiqnicClient.nodeInstance.addNeighbour(new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2])));
        }
    }

    @Override
    public Set<IPeer> addFragmentInit(FragmentBase fragment, List<TripleString> triples, int ttl) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return new HashSet<>();
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(2);
        out.println(fragment.getOwner().getAddress() + ";" + fragment.getOwner().getPort() + ";" + fragment.getOwner().getId());
        out.println(fragment.getBaseUri() + ";;" + fragment.getId() + ";;" + ttl);

        for(TripleString t : triples) {
            out.println(t.getSubject() + ";;" + t.getPredicate() + ";;" + t.getObject());
        }

        Set<IPeer> ret = new HashSet<>();
        String response;
        while ((response = in.readLine()) != null) {
            String[] words = response.split(";");
            ret.add(new Peer(words[0], Integer.parseInt(words[1]), UUID.fromString(words[2])));
        }

        socket.close();
        return ret;
    }

    private double relatedness() {
        int count;
        try {
            count = PiqnicClient.nodeInstance.getNumJoinable(getConstituents());
        } catch(IOException e) {
            count = 0;
        }

        return ((double) count) / ((double) PiqnicClient.nodeInstance.getAllFragments().size());
    }

    @Override
    public Constituents getConstituents() throws IOException {
        if(!constituents.hasConstituents()) {
            Socket socket;
            try {
                socket = new Socket(address, port);
            } catch (Exception e) {
                kill();
                return new Constituents();
            }

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(9);

            String response;
            while ((response = in.readLine()) != null) {
                constituents.addUri(response);
            }

            socket.close();
        }
        return constituents;
    }

    @Override
    public void addTriplesToFragment(FragmentBase fragment, List<Triple> triples) throws IOException  {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        out.println(3);
        out.println(fragment.getBaseUri() + ";" + fragment.getId());
        out.println(0);

        for(Triple t : triples) {
            out.println(t.getSubject() + ";;" + t.getPredicate() + ";;" + t.getObject());
        }
        socket.close();
    }

    @Override
    public void removeTriplesFromFragment(FragmentBase fragment, List<Triple> triples) throws IOException  {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            kill();
            return;
        }

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        out.println(3);
        out.println(fragment.getBaseUri() + ";" + fragment.getId());
        out.println(1);

        for(Triple t : triples) {
            out.println(t.getSubject() + ";;" + t.getPredicate() + ";;" + t.getObject());
        }
        socket.close();
    }

    private void kill() {
        PiqnicClient.nodeInstance.removePeer(this);

        for(Dataset dataset : PiqnicClient.nodeInstance.getDatasets()) {
            for(MetaFragmentBase fragment : dataset.getFragments()) {
                if(fragment.hasPeer(this)) {
                    fragment.removePeer(this);
                    Peer p = new Peer((PiqnicNode) PiqnicClient.nodeInstance);
                    try {
                        fragment.addPeers(new ArrayList<>(p.addFragmentInit(fragment, PiqnicClient.nodeInstance.getTriples(fragment), 1)));
                    } catch(IOException e) {}
                }
            }
        }

        Peer p = new Peer((PiqnicNode)PiqnicClient.nodeInstance);
        Dataset d = new Dataset(getRandomBaseUri(), p);
        int i = 0;
        for(FragmentBase fragment : PiqnicClient.nodeInstance.getAllFragments()) {
            if(fragment.ownedBy(this)) {
                i++;
                FragmentBase newFragment = FragmentFactory.createFragment(d.getUri(), fragment.getId(), fragment.getFile(), p);

                List<TripleString> triples = PiqnicClient.nodeInstance.getTriples(fragment);
                PiqnicClient.nodeInstance.removeFragment(fragment);
                PiqnicClient.nodeInstance.addFragment(newFragment);

                try {
                    d.addFragment(newFragment.toMetaFragment(p.addFragmentInit(newFragment, triples, 1)));
                } catch(IOException e) {}
            }
        }

        if(i > 0) {
            PiqnicClient.nodeInstance.addDataset(d);
        }
    }

    private String getRandomBaseUri() {
        byte[] array = new byte[7];
        new Random().nextBytes(array);
        return "http://qweb.cs.aau.dk/piqnic/" + new String(array, Charset.forName("UTF-8"));
    }

    @Override
    public int compareTo(Peer peer) {
        Double relatedness1 = relatedness();
        Double relatedness2 = peer.relatedness();

        return relatedness1.compareTo(relatedness2);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Peer peer = (Peer) o;
        return port == peer.port &&
                address.equals(peer.address) &&
                id.equals(peer.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port, id);
    }
}
