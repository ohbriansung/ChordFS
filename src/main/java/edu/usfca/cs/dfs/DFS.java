package edu.usfca.cs.dfs;

import java.net.SocketException;

public class DFS {
    static final int MAX_CHUNK_SIZE = 16 * 1024 * 1024;
    static volatile boolean alive = true;

    public static void main(String[] args) {
        try {
            Receiver receiver = new Receiver(8080, 4);

            Thread listen = new Thread(receiver);
            listen.start();
            listen.join();
        } catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
