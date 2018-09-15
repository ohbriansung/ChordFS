package edu.usfca.cs.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Sender {
    private final ExecutorService pool;

    Sender() {
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
    }

    void send(StorageMessages.Info info) {
        try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
            info.writeDelimitedTo(outStream);
            byte[] packet = outStream.toByteArray();
            DatagramPacket datagramPacket = new DatagramPacket(packet, packet.length
                    , InetAddress.getLocalHost(), 13000);

            DFS.SOCKET.send(datagramPacket);
        }
        catch (IOException ignore) {}
    }
}
