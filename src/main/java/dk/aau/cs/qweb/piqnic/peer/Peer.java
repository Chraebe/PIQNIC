package dk.aau.cs.qweb.piqnic.peer;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.util.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class Peer implements IPeer, Comparable<Peer> {
    private String address;
    private int port;
    private UUID id;

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
            return;
        }
        //System.out.println("Passing " + triple.toString() + " on to " + port + " with ttl " + ttl);

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(4);
        out.println(id.toString() + ";" + ttl + ";" + triple.getSubject() + ";" + triple.getPredicate() + ";" + triple.getObject());

        String line = null;
        while ((line = reader.readLine()) != null) {
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
            writer.println(line);
        }

        socket.close();
    }

    @Override
    public List<IPeer> shuffle(List<IPeer> peers) throws IOException {
        Socket socket;
        try {
            socket = new Socket(address, port);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(1);
        for (IPeer peer : peers) {
            out.println(peer.getId().toString() + ";" + peer.getAddress() + ";" + peer.getPort());
        }
        out.close();

        List<IPeer> ret = new ArrayList<>();
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

    private double relatedness() {
        //Todo compute
        int count = 0;

        /*for (FragmentBase fragment : PiqnicClient.nodeInstance.getAllFragments()) {
            if(fragmentPredicates.contains(fragment.getPredicate())) count++;
        }

        /*List<FragmentBase> fragments = PiqnicClient.nodeInstance.getAllFragments();
        int count = 0;
        List<FragmentConstituents> fcs = PiqnicNode.fragmentConstituents.get(id);

        int num = PiqnicClient.nodeInstance.fragmentCount();
        int n1 = fcs.size();
        for (int i = 0; i < num; i++) {
            FragmentConstituents fc = fragments.get(i).asFragment().getConstituents();
            for (int j = 0; j < n1; j++) {
                FragmentConstituents fcother = fcs.get(j);
                if (fc.joinable(fcother) && !fc.equivalent(fcother)) {
                    count++;
                    break;
                }
            }
        }*/

        return 0;
        //return ((double) count) / ((double) fragments.size());
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
