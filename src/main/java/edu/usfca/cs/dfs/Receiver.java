package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Receiver implements Runnable {
    private String host;
    private int port;
    private final ExecutorService pool;

    Receiver(String host, int port) {
        this.host = host;
        this.port = port;
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
    }

    /**
     * Usage: for closing socket.
     */
    public void close() {
        try {
            DFS.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (this.pool != null && !this.pool.isShutdown()) {
            this.pool.shutdown();
        }
    }

    @Override
    public void run() {
        try {
            printInfo();
            DFS.READY.countDown();

            while (DFS.alive) {
                Socket listening = DFS.socket.accept();
                this.pool.submit(new RequestHandler(listening));
            }
        } catch (IOException ignore) {
            // IOException will be caused by closing ServerSocket or UnknownHostException
        } finally {
            try {
                DFS.socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (!this.pool.isShutdown()) {
                this.pool.shutdown();
            }
        }
    }

    private void printInfo() {
        System.out.println("Starts receiver on " + this.host + ":" + this.port);
    }
}