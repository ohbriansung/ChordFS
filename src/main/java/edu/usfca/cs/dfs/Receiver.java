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
    void close() {
        DFS.socket.close();
    }

    @Override
    public void run() {
        // other metadata like fileName, chunkId...
        // maximum file name length is 255: https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
        int headerBytes = 512;

        try {
            printInfo();
            DFS.READY.countDown();

            while (DFS.alive) {
                byte[] bytes = new byte[DFS.MAX_CHUNK_SIZE + headerBytes];
                DatagramPacket packet = new DatagramPacket(bytes, bytes.length);

                DFS.socket.receive(packet);
                this.pool.submit(new RequestHandler(packet));
            }
        } catch (IOException ignore) {
            // IOException will be caused by closing DatagramSocket or UnknownHostException
        } finally {
            if (!DFS.socket.isClosed()) {
                DFS.socket.close();
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