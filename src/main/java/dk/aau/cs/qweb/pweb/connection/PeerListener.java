package dk.aau.cs.qweb.pweb.connection;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.aau.cs.qweb.pweb.PWebClient;
import dk.aau.cs.qweb.pweb.config.Configuration;
import dk.aau.cs.qweb.pweb.peer.IPeer;
import dk.aau.cs.qweb.pweb.peer.Peer;
import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;

import java.io.*;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

public class PeerListener implements IPeerListener {
    private final int port;
    private PrintWriter writer;
    private boolean quit = false;

    public PeerListener(int port) {
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (!quit) {
            Socket connectionSocket = serverSocket.accept();

            InputStream input = connectionSocket.getInputStream();
            OutputStream output = connectionSocket.getOutputStream();

            Scanner scanner = new Scanner(input, "UTF-8");
            writer = new PrintWriter(new OutputStreamWriter(output, "UTF-8"), true);
            processConnection(scanner);
        }
        PWebClient.running = false;
    }

    private void processConnection(Scanner scanner) {
        boolean done = false;

        /*
         * 0 [ID]                                  - Reload local datastore
         * 1 [ID] [ADDRESS] [PORT]                 - Join network
         * 2 [ID] [FRAGMENTS]                      - Response join network
         * 3 [ID] [NEIGHBOURS] [FRAGMENTS]         - Exchange information
         * 4 [ID] [NEIGHBOURS] [FRAGMENTS]         - Response exchange information
         * 5 [ID] [FRAGMENT] [TTL]                 - Add fragment
         * 6 [ID]                                  - Add fragment success
         * 7 [ID] [QUERY] [TTL]                    - Process query
         * 8 [ID] [ANSWER]                         - Answer query
         * A [ID]                                  - Request information
         * B [ID] [ADDRESS] [PORT]                 - Information response
         * C [ID]                                  - Initiate contact (ping)
         * D [ID]                                  - Contact initiated (pong)
         * E [ID] [MSG]                            - Error
         * F [ID]                                  - Quit
         */

        while (!quit && scanner.hasNextLine()) {
            String line = scanner.nextLine();
            try {
                if (line.startsWith("1")) {
                    String[] words = line.split(" ");
                    joinNetwork(UUID.fromString(words[1]), words[2], Integer.parseInt(words[3]));
                } else if (line.startsWith("0")) {
                    reloadDataset();
                } else if (line.startsWith("3")) {
                    String neighbourString = line.substring(line.indexOf("["), line.indexOf("]") + 1);
                    String fragmentsString = line.substring(line.indexOf("]") + 2);

                    Gson gson = new Gson();
                    Type peerType = new TypeToken<List<Peer>>() {
                    }.getType();
                    List<Peer> peers = gson.fromJson(neighbourString, peerType);

                    Type fragmentType = new TypeToken<List<Fragment>>() {
                    }.getType();
                    List<Fragment> fragments = gson.fromJson(fragmentsString, fragmentType);
                    exchangeInformation(peers, fragments);
                } else if (line.startsWith("5")) {
                    String fragmentString = line.substring(line.indexOf("{"), line.lastIndexOf("}") + 1);

                    Gson gson = new Gson();
                    Type type = new TypeToken<Fragment>() {
                    }.getType();
                    Fragment fragment = gson.fromJson(fragmentString, type);

                    int ttl = Integer.parseInt(line.substring(line.lastIndexOf("}") + 2));

                    addFragment(fragment, ttl);
                } else if (line.startsWith("7")) {
                    String query = line.substring(line.indexOf("'") + 1, line.lastIndexOf("'"));
                    int ttl = Integer.parseInt(line.substring(line.lastIndexOf("'") + 2));
                    queryNetwork(query, ttl);
                } else if (line.startsWith("A")) {
                    requestInformation();
                } else if (line.startsWith("C")) {
                    requestInformation();
                } else if (line.startsWith("F")) {
                    String[] words = line.split(" ");
                    quit(UUID.fromString(words[1]));
                } else
                    writer.println("E " + PWebClient.nodeInstance.getId() + " Unknown command");
            } catch (Exception e) {
                writer.println("E " + PWebClient.nodeInstance.getId() + " Unsupported operation " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Override
    public void joinNetwork(UUID id, String address, int port) {
        IPeer peer = new Peer(address, port, id, new Timestamp(System.currentTimeMillis()));
        PWebClient.nodeInstance.addPeer(peer);

        Gson gson = new Gson();
        writer.println("2 " + PWebClient.nodeInstance.getId() + " " + gson.toJson(PWebClient.nodeInstance.getRandomFragments(Configuration.instance.getReplication())));
    }

    @Override
    public void requestInformation() {
        writer.println("B " + PWebClient.nodeInstance.getId() + " " + PWebClient.nodeInstance.getIp() + " " + PWebClient.nodeInstance.getPort());
    }

    @Override
    public void ping() {
        writer.println("D " + PWebClient.nodeInstance.getId());
    }

    @Override
    public void exchangeInformation(List<Peer> peers, List<Fragment> fragments) {
        for (IPeer peer : peers) {
            PWebClient.nodeInstance.addPeer(peer);
        }
        try {
            PWebClient.nodeInstance.addFragments(fragments);
        } catch (IOException e) {
            writer.println("E " + PWebClient.nodeInstance.getId() + " Error adding fragments to node " + e.getMessage());
        }

        List<Peer> retPeers = PWebClient.nodeInstance.getRandomPeers(peers.size());
        List<Fragment> retFragments = PWebClient.nodeInstance.getRandomFragments(fragments.size());

        Gson gson = new Gson();
        writer.println("4 " + PWebClient.nodeInstance.getId() + " " + gson.toJson(retPeers) + " " + gson.toJson(retFragments));
    }

    @Override
    public void addFragment(Fragment fragment, int ttl) {
        int timeToLive = ttl;
        if(PWebClient.nodeInstance.fragmentCount() < Configuration.instance.getMaxFragments()) {
            try {
                PWebClient.nodeInstance.addFragment(fragment);
            } catch (IOException e) {
                writer.println("E " + PWebClient.nodeInstance.getId() + " Error adding fragment " + e.getMessage());
                return;
            }
        } else timeToLive = ttl + 1;

        if (timeToLive > 1) {
            int newttl = timeToLive - 1;
            if (PWebClient.nodeInstance.getPeers().size() > 0) {
                IPeer peer = PWebClient.nodeInstance.getRandomPeers(1).get(0);
                String response;
                try {
                    response = peer.addFragment(fragment, newttl);
                } catch(IOException e) {
                    return;
                }

                if (response.startsWith("E")) {
                    writer.println("E " + PWebClient.nodeInstance.getId() + " Error adding fragment to node " + ((Peer) peer).getAddress());
                    return;
                }
            }
        }

        writer.println("6 " + PWebClient.nodeInstance.getId());
    }

    @Override
    public void queryNetwork(String query, int ttl) {
        List<IPeer> peers = PWebClient.nodeInstance.getPeers();
        var wrapper = new Object() {
            QueryResult res = PWebClient.nodeInstance.processQuery(query);
            boolean free = false;
        };

        if (ttl > 1) {
            int newttl = ttl - 1;

            for (IPeer peer : peers) {
                Runnable runnable = () -> {
                    QueryResult intermediateRes;
                    try {
                        intermediateRes = peer.query(query, newttl);
                    } catch (IOException e) {
                        return;
                    }
                    while (!wrapper.free) ;

                    wrapper.free = false;
                    Iterator<Map<String, String>> iterator = intermediateRes.iterator();

                    while (iterator.hasNext()) {
                        wrapper.res.addResultRow(iterator.next());
                    }
                    wrapper.free = true;
                };

                Thread t = new Thread(runnable);
                t.start();
            }
        }

        Gson gson = new Gson();
        writer.println("8 " + PWebClient.nodeInstance.getId() + " " + gson.toJson(wrapper.res));
    }

    @Override
    public void quit(UUID id) {
        if (id.equals(PWebClient.nodeInstance.getId()))
            quit = true;
    }

    @Override
    public void reloadDataset() throws IOException {
        PWebClient.nodeInstance.reloadDatastore();

        for (IPeer peer : PWebClient.nodeInstance.getPeers()) {
            Runnable runnable = () -> {
                try {
                    peer.reloadDatastore();
                } catch (IOException e) {
                    return;
                }
            };

            Thread t = new Thread(runnable);
            t.start();
        }
    }
}
