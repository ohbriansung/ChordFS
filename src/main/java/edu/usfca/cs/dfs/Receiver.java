package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Receiver implements Runnable {
    private String host;
    private int port;
    private final ExecutorService pool;
    private final AtomicInteger requestCounter;

    Receiver(String host, int port) {
        this.host = host;
        this.port = port;
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
        this.requestCounter = new AtomicInteger();
    }

    void close() {
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
                this.requestCounter.incrementAndGet();
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

    public int getRequestCount() {
        return this.requestCounter.get();
    }

    private void printInfo() {
        System.out.println("Starts receiver on " + this.host + ":" + this.port);
    }
}