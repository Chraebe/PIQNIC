package dk.aau.cs.qweb.pweb.cli;

import java.io.IOException;
import java.util.Scanner;

public interface IClient {
    void start() throws IOException;
    void startup(Scanner scanner) throws IOException;
    void addHDTFile(String filename) throws IOException;
    void queryNetwork(String query) throws IOException;
    void quit() throws IOException;
    void help();
}