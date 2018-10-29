package dk.aau.cs.qweb.pweb.cli;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.aau.cs.qweb.pweb.PWebClient;
import dk.aau.cs.qweb.pweb.config.Configuration;
import dk.aau.cs.qweb.pweb.peer.Peer;
import dk.aau.cs.qweb.pweb.util.Dataset;
import dk.aau.cs.qweb.pweb.util.Fragment;
import dk.aau.cs.qweb.pweb.util.QueryResult;
import dk.aau.cs.qweb.pweb.util.Triple;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.DictionaryUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressOut;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.*;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;

import static dk.aau.cs.qweb.pweb.PWebClient.nodeInstance;

public class Client implements IClient {
    private static boolean startup = true;
    private final int port;
    private PrintWriter writer;
    private boolean quit = false;
    private Socket connectionSocket;

    public Client(int port) {
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (!quit) {
            connectionSocket = serverSocket.accept();

            InputStream input = connectionSocket.getInputStream();
            OutputStream output = connectionSocket.getOutputStream();

            Scanner scanner = new Scanner(input, "UTF-8");
            writer = new PrintWriter(new OutputStreamWriter(output, "UTF-8"), true);
            processConnection(scanner);
        }
    }

    private void processConnection(Scanner scanner) throws IOException {
        if (startup) startup(scanner);
        writer.println("Welcome to pWeb. Write :h or :help for help");
        while (!quit) {
            String line = scanner.nextLine();

            if (line.startsWith(":")) processCommand(line);
            else processQuery(line, scanner);
        }
    }

    private void processCommand(String line) throws IOException {
        if (line.equals(":q") || line.equals(":quit")) quit();
        else if (line.equals(":h") || line.equals(":help")) help();
        else if (line.startsWith(":l") && line.contains(" ")) {
            String[] words = line.split(" ");
            addHDTFile(words[1]);
        }
    }

    private void processQuery(String line, Scanner scanner) throws IOException {
        String query = line;
        while (!query.endsWith(";")) {
            query = query + " " + scanner.nextLine();
        }
        query = query.substring(0, query.length() - 1);
        queryNetwork(query);
    }

    @Override
    public void startup(Scanner scanner) throws IOException {
        writer.println("Please enter the address and port of a node to connect to the network like so: [ADDRESS] [PORT]");
        String input = scanner.nextLine();

        if (input.equals("0")) return;

        String[] words = input.split(" ");
        Socket socket = new Socket(words[0], Integer.parseInt(words[1]));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println("1 " + PWebClient.nodeInstance.getId() + " " + PWebClient.nodeInstance.getIp() + " " + PWebClient.nodeInstance.getPort());
        String response = in.readLine();

        if (response.startsWith("2")) {
            String[] w = response.split(" ");
            UUID id = UUID.fromString(w[1]);
            PWebClient.nodeInstance.addPeer(new Peer(words[0], Integer.parseInt(words[1]), id, new Timestamp(System.currentTimeMillis())));

            String string = response.substring(response.indexOf("["), response.lastIndexOf("]") + 1);
            Gson gson = new Gson();
            Type type = new TypeToken<List<Fragment>>() {
            }.getType();
            List<Fragment> fragments = gson.fromJson(string, type);

            PWebClient.nodeInstance.addFragments(fragments);
        } else if (response.startsWith("E")) {
            socket.close();
            writer.println("Unknown node");
            startup(scanner);
        }
        socket.close();
    }

    @Override
    public void addHDTFile(String filename) throws IOException {
        HDT hdt = HDTManager.loadIndexedHDT(filename, ProgressOut.getInstance());

        Dictionary dictionary = hdt.getDictionary();
        DictionarySection predicateDictionary = dictionary.getPredicates();

        Dataset dataset = new Dataset(filename);
        for (int i = 1; i < predicateDictionary.getNumberOfElements() + 1; i++) {
            String pred = charSequenceToString(predicateDictionary.extract(i));
            IteratorTripleID iterator = hdt.getTriples().search(new TripleID(0, dictionary.stringToId(pred, TripleComponentRole.PREDICATE), 0));

            Fragment fragment = new Fragment(pred);
            while (iterator.hasNext()) {
                TripleString triple = DictionaryUtil.tripleIDtoTripleString(dictionary, iterator.next());
                fragment.addTriple(new Triple(charSequenceToString(triple.getSubject()), pred, charSequenceToString(triple.getObject())));
            }
            dataset.addFragment(fragment);
        }

        Socket socket = new Socket(nodeInstance.getIp(), nodeInstance.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        Iterator<Fragment> iterator = dataset.getFragments();
        Gson gson = new Gson();
        while (iterator.hasNext()) {
            Fragment f = iterator.next();
            out.println("5 " + nodeInstance.getId() + " " + gson.toJson(f) + " " + Configuration.instance.getReplication());

            String response = in.readLine();
            if (response.startsWith("6")) {
                System.out.println("Added fragment with predicate " + f.getPredicate());
            } else if (response.startsWith("E")) {
                System.out.println("Error!");
            }
        }

        out.println("0 " + nodeInstance.getId());
        socket.close();
        writer.println("Done adding fragments...");
    }

    @Override
    public void queryNetwork(String query) throws IOException {
        Socket socket = new Socket(nodeInstance.getIp(), nodeInstance.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println("7 " + nodeInstance.getId() + " '" + query + "' " + Configuration.instance.getTimeToLive());
        String response = in.readLine();

        if (response.startsWith("E")) {
            System.out.println("An error occured");
            socket.close();
            return;
        }

        if (response.startsWith("8")) {
            String resString = response.substring(response.indexOf("{"));

            Gson gson = new Gson();
            Type type = new TypeToken<QueryResult>() {
            }.getType();
            QueryResult result = gson.fromJson(resString, type);

            // Print header
            List<String> heads = result.getHeads();
            List<Map<String, String>> rows = result.getRows();
            int[] sizes = new int[heads.size()];

            for (int i = 0; i < heads.size(); i++) {
                int size = 0;
                //for(int j = 0; j < rows.size(); j++) {
                    for (String value : rows.get(i).values()) {
                        if (value.length() > size) size = value.length();
                    }
                    for (String value : rows.get(i).keySet()) {
                        if (value.length() > size) size = value.length();
                    }
                //}
                sizes[i] = size;
            }

            String format = "|";
            String border = "+";
            for (int i = 0; i < heads.size(); i++) {
                format += " %-" + sizes[i] + "s |";
                for (int j = 0; j < sizes[i]; j++) {
                    border += "-";
                }
                border += "--+";
            }
            format += "%n";
            border += "%n";

            writer.format(border);
            String[] headers = new String[heads.size()];
            headers = heads.toArray(headers);
            writer.format(format, headers);
            writer.format(border);

            // Write content
            Iterator<Map<String, String>> rowIterator = result.iterator();
            while (rowIterator.hasNext()) {
                String[] elements = new String[heads.size()];
                for (int i = 0; i < elements.length; i++) {
                    elements[i] = result.getValue(heads.get(i));
                }
                writer.format(format, elements);
                rowIterator.next();
            }
            writer.format(border);


            //writer.println(result.toString());
        }

        socket.close();
    }

    @Override
    public void quit() throws IOException {
        connectionSocket.close();
    }

    @Override
    public void help() {
        writer.println("This is pWeb version " + Configuration.instance.getVersion() + ". Write a query and end with ';' to query the network. \nOtherwise the following commands work:");
        writer.println();
        writer.println(":q or :quit - quits the client. The node will still be on the network.");
        writer.println(":h or :help - prints this message.");
        writer.println(":l or :load [/path/to/file.hdt] - loads file.hdt into the network.");
    }

    private static String charSequenceToString(CharSequence charSequence) {
        final StringBuilder sb = new StringBuilder(charSequence.length());
        sb.append(charSequence);
        return sb.toString();
    }
}
