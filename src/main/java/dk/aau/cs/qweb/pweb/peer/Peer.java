package dk.aau.cs.qweb.pweb.peer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.aau.cs.qweb.pweb.PWebClient;
import dk.aau.cs.qweb.pweb.node.PWebNode;
import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import static dk.aau.cs.qweb.pweb.PWebClient.nodeInstance;

public class Peer implements IPeer {
    private String address;
    private int port;
    private UUID id;
    private Timestamp contactInitiated;

    public Peer(String address, int port, UUID id, Timestamp time) {
        this.address = address;
        this.port = port;
        this.id = id;
        this.contactInitiated = time;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public UUID getId() {
        return id;
    }

    public Timestamp getContactInitiated() {
        return contactInitiated;
    }

    @Override
    public QueryResult query(String sparql, int timeToLive) throws IOException {
        Socket socket = new Socket(address, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println("7 " + PWebClient.nodeInstance.getId() + " " + sparql + " " + timeToLive);
        String response = in.readLine();
        if(response.startsWith("E")) {
            socket.close();
            return null;
        }

        if(response.startsWith("8")) {
            String resString = response.substring(response.indexOf("{"));

            Gson gson = new Gson();
            Type type = new TypeToken<QueryResult>() {}.getType();
            QueryResult result = gson.fromJson(resString, type);
            socket.close();
            return result;
        }

        socket.close();
        return null;
    }

    @Override
    public String exchangeInformation(List<Peer> peers, List<Fragment> fragments) throws IOException {
        Socket socket = new Socket(address, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        Gson gson = new Gson();
        out.println("3 " + PWebClient.nodeInstance.getId() + " " + gson.toJson(peers) + " " + gson.toJson(fragments));
        String response = in.readLine();

        socket.close();
        return response;
    }

    @Override
    public String addFragment(Fragment fragment, int timeToLive) throws IOException {
        Socket socket = new Socket(address, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        Gson gson = new Gson();
        out.println("5 " + PWebClient.nodeInstance.getId() + " " + gson.toJson(fragment) + " " + timeToLive);
        String response = in.readLine();

        socket.close();
        return response;
    }

    @Override
    public void reloadDatastore() throws IOException {
        Socket socket = new Socket(address, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println("0 " + PWebClient.nodeInstance.getId());
        socket.close();
    }
}
