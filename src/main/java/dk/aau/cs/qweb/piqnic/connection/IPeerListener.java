package dk.aau.cs.qweb.piqnic.connection;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public interface IPeerListener {
    void start() throws IOException;

    void join(Scanner scanner, PrintWriter writer);

    void shuffle(Scanner scanner, PrintWriter writer);

    void addFragment(Scanner scanner, PrintWriter writer);

    void updateFragment(Scanner scanner, PrintWriter writer);

    void processTriplePattern(Scanner scanner, PrintWriter writer);

    void processTriplePatternBound(Scanner scanner, PrintWriter writer);

    void estimateCardinality(Scanner scanner, PrintWriter writer);

    void passJoin(Scanner scanner, PrintWriter writer);
}
