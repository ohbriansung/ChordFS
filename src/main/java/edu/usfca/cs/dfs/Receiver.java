package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Receiver implements Runnable {
    private final ExecutorService pool;
    private final DatagramSocket socket;

    Receiver(int port) throws SocketException {
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
        this.socket = new DatagramSocket(port);
    }

    /**
     * Usage: for closing socket.
     */
    void close() {
        this.socket.close();
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

                this.socket.receive(packet);
                this.pool.submit(new RequestHandler(packet));
            }
        } catch (IOException ignore) {
            // IOException will be caused by closing DatagramSocket or UnknownHostException
        } finally {
            if (!this.socket.isClosed()) {
                this.socket.close();
            }

            if (!this.pool.isShutdown()) {
                this.pool.shutdown();
            }
        }
    }

    private void printInfo() throws UnknownHostException {
        String hostName = InetAddress.getLocalHost().getHostName();
        int port = this.socket.getLocalPort();
        System.out.println("Starts receiver on " + hostName + ":" + port);
    }
}