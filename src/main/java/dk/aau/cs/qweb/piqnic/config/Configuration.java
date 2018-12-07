package dk.aau.cs.qweb.piqnic.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Type;

public class Configuration {
    public static Configuration instance;

    private final int replication;
    private int listenerPort;
    private int cliPort;
    private int testPort;
    private final int maxFragments;
    private int timeToLive;
    private final String version;
    private final int shuffleLength;
    private int neighbours;
    private final int minutesTilShuffle;
    private final int maxDelay;

    public Configuration(String filename) throws IOException {
        File file = new File(filename);
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String json = new String(data, "UTF-8");

        Gson gson = new Gson();
        Type type = new TypeToken<Config>() {
        }.getType();
        Config config = gson.fromJson(json, type);

        this.replication = config.replication;
        this.listenerPort = config.ports.listener;
        this.cliPort = config.ports.cli;
        this.testPort = config.ports.test;
        this.maxFragments = config.maxFragments;
        this.timeToLive = config.timeToLive;
        this.version = config.version;
        this.shuffleLength = config.shuffleLength;
        this.neighbours = config.neighbours;
        this.minutesTilShuffle = config.minutesTilShuffle;
        this.maxDelay = config.maxDelay;
    }

    public void setListenerPort(int port) {this.listenerPort = port;}
    public void setCliPort(int port) {this.cliPort = port;}

    public void setNeighbours(int neighbours) {
        this.neighbours = neighbours;
    }

    public void setTestPort(int testPort) {
        this.testPort = testPort;
    }

    public int getMaxDelay() {
        return maxDelay;
    }

    public int getTestPort() {
        return testPort;
    }

    public int getShuffleLength() {
        return shuffleLength;
    }

    public int getMinutesTilShuffle() {
        return minutesTilShuffle;
    }

    public int getNeighbours() {
        return neighbours;
    }

    public int getReplication() {
        return replication;
    }

    public int getListenerPort() {
        return listenerPort;
    }

    public int getCliPort() {
        return cliPort;
    }

    public int getMaxFragments() {
        return maxFragments;
    }

    public int getTimeToLive() {
        return timeToLive;
    }

    public void setTTL(int ttl) {
        this.timeToLive = ttl;
    }

    public String getVersion() {
        return version;
    }

    private class Config {
        int replication;
        Ports ports;
        int maxFragments;
        int timeToLive;
        String version;
        int shuffleLength;
        int minutesTilShuffle;
        int neighbours;
        int maxDelay;
    }

    private class Ports {
        int listener;
        int cli;
        int test;
    }
}
