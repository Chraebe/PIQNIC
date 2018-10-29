package dk.aau.cs.qweb.pweb.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Type;

public class Configuration {
    public static Configuration instance;

    private final int replication;
    private final int listenerPort;
    private final int cliPort;
    private final int maxFragments;
    private final int timeToLive;
    private final String version;

    public Configuration(String filename) throws IOException {
        File file = new File(filename);
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String json = new String(data, "UTF-8");

        Gson gson = new Gson();
        Type type = new TypeToken<Config>() {}.getType();
        Config config = gson.fromJson(json, type);

        this.replication = config.replication;
        this.listenerPort = config.ports.listener;
        this.cliPort = config.ports.cli;
        this.maxFragments = config.maxFragments;
        this.timeToLive = config.timeToLive;
        this.version = config.version;
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

    public String getVersion() {
        return version;
    }

    private class Config {
        int replication;
        Ports ports;
        int maxFragments;
        int timeToLive;
        String version;
    }

    private  class Ports {
        int listener;
        int cli;
    }
}
