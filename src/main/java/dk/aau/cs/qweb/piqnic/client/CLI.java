package dk.aau.cs.qweb.piqnic.client;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.FragmentFactory;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.graph.PiqnicGraph;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.DictionaryUtil;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressOut;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.*;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.*;

import static dk.aau.cs.qweb.piqnic.PiqnicClient.nodeInstance;
import static dk.aau.cs.qweb.piqnic.PiqnicClient.test;

public class CLI implements IClient {
    private static boolean startup = true;
    private final int port;
    private boolean quit = false;
    private ServerSocket serverSocket;
    private Gson gson = new Gson();

    public CLI(int port) {
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        while (!quit) {
            Socket connectionSocket = serverSocket.accept();
            Thread t = new ClientThread(connectionSocket);
            t.start();
        }
    }

    private class ClientThread extends Thread {
        private final Socket connectionSocket;
        private PrintWriter writer;

        ClientThread(Socket socket) {
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

            try {
                processConnection(scanner, writer);
            } catch (IOException e) {
                return;
            }

            writer.close();
            scanner.close();
            try {
                input.close();
                output.close();
                connectionSocket.close();
            } catch (IOException e) {
            }
        }
    }

    private void processConnection(Scanner scanner, PrintWriter writer) throws IOException {
        if (!test) {
            if (startup) startup(scanner);
        }
        startup = false;
        writer.println("Welcome to PIQNIC. Write :h or :help for help");
        while (!quit) {
            try {
                String line = scanner.nextLine();
                if (line.startsWith("\n")) line = line.replaceFirst("\n", "");

                if (line.startsWith(":")) processCommand(line);
                else processQuery(line, scanner);
            } catch (NoSuchElementException e) {
                return;
            }
        }
    }

    private void processCommand(String line) throws IOException {
        if (line.equals(":q") || line.equals(":quit")) quit();
        else if (line.equals(":h") || line.equals(":help")) help();
        else if (line.startsWith(":l") && line.contains(" ")) {
            String[] words = line.split(" ");
            if (words[1].endsWith(".hdt"))
                addHDTFile(words[1]);
            else
                addRDFFile(words[1]);
            //fragments();
        } else if (line.startsWith(":m")) metadata();
        else if (line.startsWith(":f")) fragments();
        else if (line.startsWith(":n")) neighbours();
        else if (line.startsWith(":s")) shuffle();
        else if (line.startsWith(":d")) datasets();
        else if (line.startsWith(":c")) constituents();
        else help();
    }

    private void constituents() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        for (IPeer peer : PiqnicClient.nodeInstance.getNeighbours()) {
            writer.println("For peer " + peer.getPort());
            System.out.println("Peer " + peer.getPort());
            try {
                peer.getConstituents();
            } catch (IOException e) {
            }
            System.out.println();
        }
        writer.println("Done.");
    }

    private void shuffle() {
        try {
            PiqnicClient.nodeInstance.shuffle();
        } catch (IOException e) {
        }
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        writer.println("Shuffle complete.");
    }

    private void metadata() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        writer.println("ID: " + PiqnicClient.nodeInstance.getId());
        writer.println("IP Address: " + PiqnicClient.nodeInstance.getIp());
        writer.println("Port: " + PiqnicClient.nodeInstance.getPort());
        writer.println("No. of Fragments: " + PiqnicClient.nodeInstance.fragmentCount());
        writer.println("No. of Neighbours: " + PiqnicClient.nodeInstance.getNeighbours().size());
    }

    private void fragments() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        long num = 0;
        for (FragmentBase f : PiqnicClient.nodeInstance.getAllFragments()) {
            long res = PiqnicClient.nodeInstance.estimateResult(new Triple("ANY", f.getId(), "ANY"));
            writer.println(f.getBaseUri() + " : " + f.getId() + " : " + res);
            num += res;
        }

        writer.println();
        writer.println("Total no. of fragments: " + PiqnicClient.nodeInstance.fragmentCount() + ", total no. of triples: " + num);

        /*try {
            PiqnicClient.nodeInstance.reloadDatastore();
        } catch(IOException e) {}*/
    }

    private void datasets() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        for (Dataset fd : PiqnicClient.nodeInstance.getDatasets()) {
            writer.println(fd.getUri() + " : " + fd.getFragments().size());
        }

        /*try {
            PiqnicClient.nodeInstance.reloadDatastore();
        } catch(IOException e) {}*/
    }

    private void neighbours() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        for (IPeer ip : PiqnicClient.nodeInstance.getNeighbours()) {
            Peer p = (Peer) ip;
            writer.println(p.getId() + " " + p.getAddress() + ":" + p.getPort());
        }

        writer.println();
        writer.println("Total no. of neighbours: " + PiqnicClient.nodeInstance.getNeighbours().size());
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
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        writer.println("Please enter the address and port of a node to connect to the network like so: [ADDRESS] [PORT]");
        String input = scanner.nextLine();

        if (input.equals("0")) {
            PiqnicClient.manager.start();
            return;
        }

        String[] words = input.split(" ");
        Peer p = new Peer(words[0], Integer.parseInt(words[1]), UUID.randomUUID());
        p.join(new Peer((PiqnicNode)PiqnicClient.nodeInstance));
    }

    @Override
    public void addHDTFile(String filename) throws IOException {
        HDT hdt = HDTManager.mapHDT(filename, ProgressOut.getInstance());
        createDataset(hdt);
        hdt.close();
    }

    @Override
    public void addRDFFile(String filename) throws IOException {
        HDT hdt;
        try {
            hdt = HDTManager.generateHDT(filename, "http://qweb.cs.aau.dk/piqnic/", RDFNotation.guess(filename), new HDTSpecification(), ProgressOut.getInstance());
        } catch (ParserException e) {
            System.out.println("Failed to add file: " + e.getMessage());
            return;
        }
        createDataset(hdt);
        hdt.close();
    }

    private String getRandomBaseUri() {
        byte[] array = new byte[7];
        new Random().nextBytes(array);
        return "http://qweb.cs.aau.dk/piqnic/" + new String(array, Charset.forName("UTF-8"));
    }

    private void createDataset(HDT hdt) throws IOException {
        Peer p = new Peer((PiqnicNode) PiqnicClient.nodeInstance);
        Dataset dataset = new Dataset(getRandomBaseUri(), p);

        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        Dictionary dictionary = hdt.getDictionary();
        DictionarySection predicateDictionary = dictionary.getPredicates();

        for (int i = 1; i < predicateDictionary.getNumberOfElements() + 1; i++) {
            String pred = charSequenceToString(predicateDictionary.extract(i));
            writer.println("Adding fragment with predicate " + pred);

            IteratorTripleID iterator = hdt.getTriples().search(new TripleID(0, dictionary.stringToId(pred, TripleComponentRole.PREDICATE), 0));

            FragmentBase fragment = FragmentFactory.createFragment(dataset.getUri(), pred, new File("null.hdt"), p);
            List<TripleString> triples = new ArrayList<>();
            while (iterator.hasNext()) {
                triples.add(DictionaryUtil.tripleIDtoTripleString(dictionary, iterator.next()));
            }
            Set<IPeer> peers = p.addFragmentInit(fragment, triples, Configuration.instance.getReplication());
            MetaFragmentBase base = fragment.toMetaFragment(peers);
            dataset.addFragment(base);
        }

        PiqnicClient.nodeInstance.addDataset(dataset);
        writer.println("Done adding fragments...");
    }

    private void writeResults(final PrintWriter outputStream, Query query, Model model) {
        long startTime = System.currentTimeMillis();
        final QueryExecution executor = QueryExecutionFactory.create(query, model);
        int num = 0;
        switch (query.getQueryType()) {
            case Query.QueryTypeSelect:
                final ResultSet rs = executor.execSelect();
                while (rs.hasNext()) {
                    outputStream.println(rs.next());
                    num++;
                }
                break;
            case Query.QueryTypeAsk:
                outputStream.println(executor.execAsk());
                break;
            case Query.QueryTypeConstruct:
            case Query.QueryTypeDescribe:
                final Iterator<org.apache.jena.graph.Triple> triples = executor.execConstructTriples();
                while (triples.hasNext())
                    outputStream.println(triples.next());
                break;
            default:
                throw new Error("Unsupported query type");
        }

        long endTime = System.currentTimeMillis();
        outputStream.println("Done in " + (endTime - startTime) + "ms. Found " + num + " results. No. of Messages=" + PiqnicJenaConstants.NM);
    }

    @Override
    public void queryNetwork(String sparql) throws IOException {
        PiqnicJenaConstants.PROCESSOR = PiqnicJenaConstants.ProcessingType.FLOOD;
        PiqnicJenaConstants.NM = 0;
        //System.out.println(sparql);
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        Query query = QueryFactory.create(sparql);
        final PiqnicGraph graph;
        graph = new PiqnicGraph();
        writeResults(writer, query, ModelFactory.createModelForGraph(graph));

        //PiqnicClient.nodeInstance.reloadDatastore();

        //QueryResult result = PiqnicClient.nodeInstance.processQuery(query);

        // Print header
        //List<String> heads = result.getHeads();
        //List<Map<String, String>> rows = result.getRows();
        //int[] sizes = new int[heads.size()];

        /*if(rows.size() == 0) {
            writer.println("No results found...");
            return;
        }

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
        writer.format(border);*/
        //writer.println(gson.toJson(rows));
    }

    @Override
    public void quit() throws IOException {
        quit = true;
    }

    @Override
    public void help() {
        PrintWriter writer = ((ClientThread) Thread.currentThread()).writer;
        writer.println("This is PIQNIC version " + Configuration.instance.getVersion() + ". Write a processTriple and end with ';' to processTriple the network. \nOtherwise the following commands work:");
        writer.println();
        writer.println(":q or :quit - quits the client. The node will still be on the network.");
        writer.println(":h or :help - prints this message.");
        writer.println(":l or :load [/path/to/file.hdt] - loads file.hdt (or RDF) into the network.");
        writer.println(":m - Type out metadata.");
        writer.println(":f - Type out fragments.");
        writer.println(":n - Type out neighbours.");
    }

    private static String charSequenceToString(CharSequence charSequence) {
        final StringBuilder sb = new StringBuilder(charSequence.length());
        sb.append(charSequence);
        return sb.toString();
    }
}
