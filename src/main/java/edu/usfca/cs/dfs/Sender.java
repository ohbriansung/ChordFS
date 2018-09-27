package edu.usfca.cs.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Sender {
    private final ExecutorService pool;

    Sender() {
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
    }

    void send(StorageMessages.Message message, InetSocketAddress address) {
        try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
            message.writeDelimitedTo(outStream);
            byte[] bytes = outStream.toByteArray();
            DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address);

            this.pool.submit(new Task(packet));
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    void heartbeat(StorageMessages.Message message, InetSocketAddress address) throws IOException {
        DatagramPacket packet = null;

        try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
            message.writeDelimitedTo(outStream);
            byte[] bytes = outStream.toByteArray();
            packet = new DatagramPacket(bytes, bytes.length, address);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }

        if (packet != null) {
            DFS.socket.send(packet);
        }
    }

    void upload() {}

    void close() {
        if (!this.pool.isShutdown()) {
            this.pool.shutdown();
        }
    }

    private class Task implements Runnable {
        private final DatagramPacket packet;

        private Task(DatagramPacket packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            try {
                DFS.socket.send(this.packet);
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
