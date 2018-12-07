package dk.aau.cs.qweb.piqnic.test;

import dk.aau.cs.qweb.piqnic.client.IClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.graph.PiqnicGraph;
import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicJenaFloodIterator;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.rdfhdt.hdt.exceptions.NotImplementedException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class TestClient implements IClient {
    private static boolean startup = true;
    private final int port;
    private boolean quit = false;
    private ServerSocket serverSocket;
    private static int numResults = 0;

    public TestClient(int port) {
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        while (!quit) {
            Socket connectionSocket = serverSocket.accept();
            Thread t = new TestClient.ClientThread(connectionSocket);
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
                processConnection(writer, scanner);
            } catch (Exception e) {
                return;
            }

            writer.close();
            try {
                output.close();
                connectionSocket.close();
            } catch (IOException e) {
            }
        }
    }

    private void processConnection(PrintWriter writer, Scanner scanner) throws Exception {
        /*queries/S1 [flood|bind|down]*/

        String line = scanner.nextLine();
        if (line == null) return;

        String[] words = line.split(" ");
        File queryFile = new File(words[0]);
        String approach = words[1];

        PiqnicJenaConstants.PROCESSOR = PiqnicJenaConstants.ProcessingType.fromString(approach);
        writer.println(PiqnicJenaConstants.PROCESSOR);

        performTests(writer, queryFile, approach);
    }

    private void performTests(PrintWriter writer, File queryFile, String approach) throws Exception {
        File outDir = new File("experiments/approach/run1/"+queryFile.getName());
        outDir.mkdirs();
        File output = new File("experiments/approach/run1/"+queryFile.getName()+"/"+approach+".txt");
        String sparql = new String(Files.readAllBytes(Paths.get(queryFile.getAbsolutePath())), StandardCharsets.UTF_8);
        String warmup = new String(Files.readAllBytes(Paths.get("queries/S2")), StandardCharsets.UTF_8);

        writer.println("Doing warmup...");
        process(warmup, output, false, 2);
        //writer.println("Doing warmup...");
        //process(sparql, output, false, 2);
        //writer.println("Doing warmup...");
        //process(sparql, output, false, 2);
        writer.println("Warmup done, performing test");
        process(sparql, output, true, 15);
        writer.println("Done..");
        writer.close();
    }

    private void process(String sparql, File output, boolean log, int delay) throws Exception {
        PrintWriter writer = new PrintWriter(new FileOutputStream(output), true);

        Query query = QueryFactory.create(sparql);
        final PiqnicGraph graph = new PiqnicGraph();
        Model model = ModelFactory.createModelForGraph(graph);
        final QueryExecution executor = QueryExecutionFactory.create(query, model);

        ExecutorService ex = Executors.newSingleThreadExecutor();
        Future<String> future = ex.submit(new QueryTask(executor, writer, log));
        PiqnicJenaConstants.NTB = 0;

        try {
            future.get(delay, TimeUnit.MINUTES);
        } catch (Exception e) {
            future.cancel(true);
            if(log) writer.println(numResults + ";" + 0 + ";" + PiqnicJenaConstants.NTB + ";" + PiqnicJenaConstants.NM);
        }
    }

    class QueryTask implements Callable<String> {
        private QueryExecution executor;
        private PrintWriter writer;
        private boolean log;

        QueryTask(QueryExecution executor, PrintWriter writer, boolean log) {
            this.executor = executor;
            this.writer = writer;
            this.log = log;
        }

        @Override
        public String call() throws Exception {
            numResults = 0;
            long start = System.currentTimeMillis();
            final ResultSet rs = executor.execSelect();
            while (rs.hasNext()) {
                QuerySolution sol = rs.next();
                if(log) writer.println(sol);
                numResults++;
            }
            long end = System.currentTimeMillis();
            if(log) writer.println(numResults + ";" + (end-start) + ";" + PiqnicJenaConstants.NTB + ";" + PiqnicJenaConstants.NM);
            return "";
        }
    }

    private List<File> listFilesForFolder(final File folder) {
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

    @Override
    public void startup(Scanner scanner) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void addHDTFile(String filename) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void addRDFFile(String filename) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void queryNetwork(String sparql) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void quit() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void help() {
        throw new NotImplementedException();
    }

    private static String charSequenceToString(CharSequence charSequence) {
        final StringBuilder sb = new StringBuilder(charSequence.length());
        sb.append(charSequence);
        return sb.toString();
    }
}
