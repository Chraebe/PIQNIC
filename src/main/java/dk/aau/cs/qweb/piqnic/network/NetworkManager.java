package dk.aau.cs.qweb.piqnic.network;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NetworkManager extends Thread {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Override
    public void run() {
        System.out.println("Network manager starting...");
        Runnable runnable =
                () -> {
                    try {
                        System.out.println("Performing shuffle");
                        PiqnicClient.nodeInstance.shuffle();
                    } catch (IOException e) {
                        System.err.println("Failed to perform shuffle: " + e.getMessage());
                    }
                };

        Random rnd = new Random();
        int l = rnd.nextInt(Configuration.instance.getMinutesTilShuffle());
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {}
        try {
            System.out.println("Performing shuffle");
            PiqnicClient.nodeInstance.shuffle();
        } catch (IOException e) {
            System.err.println("Failed to perform shuffle: " + e.getMessage());
        }
        scheduler.scheduleAtFixedRate(runnable, l, Configuration.instance.getMinutesTilShuffle(), TimeUnit.MINUTES);
    }
}
