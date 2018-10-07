package edu.usfca.cs.dfs;

import edu.usfca.cs.dfs.Client.Client;
import edu.usfca.cs.dfs.Storage.Stabilization;
import edu.usfca.cs.dfs.Storage.StorageNode;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class DFS {
    static final int THREAD = 16;  // both sender and receiver will have 16 threads for handling requests
    public static final int MAX_CHUNK_SIZE = 64 * 1024 * 1024;  // 64 mb for a chunk
    public static final CountDownLatch READY = new CountDownLatch(1);  // ui waits for receiver and sender to start
    public static volatile boolean alive = true;

    static ServerSocket socket;
    public static Sender currentNode;
    public static Receiver receiver;
    public static String volume;

    public static void main(String[] args) {
        Map<String, String> arguments = parseArgs(args);
        checkArgs(arguments);

        // default arguments in case user doesn't indicate
        String host = "localhost";
        int port = 13000;
        int m = 4;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
            port = (arguments.containsKey("port") ? Integer.parseInt(arguments.get("port")) : port);
            m = (arguments.containsKey("m") ? Integer.parseInt(arguments.get("m")) : m);
            DFS.socket = new ServerSocket(port);
        } catch (NumberFormatException | IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        startReceiver(host, port);
        startNode(arguments, host, port, m);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < args.length - 1; i += 2) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                map.put(key, args[i + 1]);
            }
        }

        return map;
    }

    private static void checkArgs(Map<String, String> arguments) {
        boolean checked = true;

        if (!arguments.containsKey("run")) {
            checked = false;
        }
        else if (!arguments.containsKey("port")) {
            checked = false;
        }

        if (!checked) {
            StringBuilder sb = new StringBuilder();
            sb.append("Usage: java dfs.jar ");
            sb.append("--run <storage/client> --port <port> ");
            sb.append("[--m <m_bits_ring>] [--node <existing_node_address:port>]");
            System.out.println(sb.toString());
            System.exit(-1);
        }
    }

    private static void startReceiver(String host, int port) {
        Receiver receiver = new Receiver(host, port);
        Thread listen = new Thread(receiver);
        listen.start();
        DFS.receiver = receiver;
    }

    /**
     * Start Storage or Client node base on the argument.
     * @param arguments
     * @param host
     * @param port
     * @param m
     */
    private static void startNode(Map<String, String> arguments, String host, int port, int m) {
        if (arguments.get("run").equals("client")) {
            String[] address = arguments.get("node").split(":");
            DFS.currentNode = new Client(new InetSocketAddress(address[0], Integer.parseInt(address[1])));
            ((Client) DFS.currentNode).startUI();
        }
        else {
            DFS.volume = arguments.getOrDefault("volume", System.getProperty("user.home") + '/');
            if (!DFS.volume.endsWith("/")) {
                DFS.volume += "/";
            }
            checkVolume(DFS.volume);

            if (arguments.containsKey("node")) {
                DFS.currentNode = new StorageNode(host, port);

                String[] address = arguments.get("node").split(":");
                InetSocketAddress np = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
                try {
                    ((StorageNode) DFS.currentNode).join(np);
                    ((StorageNode) DFS.currentNode).tellSuccToBackup();
                } catch (IOException ignore) {
                    System.out.println("Node [" + np + "] or successor is unreachable, please try again with another node.");
                    DFS.shutdown();
                    System.exit(-1);
                }
            }
            else {
                DFS.currentNode = new StorageNode(host, port, m);
                ((StorageNode) DFS.currentNode).create();
            }

            Thread stabilization = new Thread(new Stabilization(((StorageNode) DFS.currentNode)));
            stabilization.start();
        }
    }

    /**
     * Check if volume exists or not.
     * @param volume
     */
    private static void checkVolume(String volume) {
        File temp = new File(volume);
        if (temp.exists() && temp.isDirectory()) {
            System.out.println("Set volume = [" + temp + "]");
        }
        else {
            System.out.println("Volume [" + temp + "] dose not exist or is not a directory.");
            System.exit(-1);
        }
    }

    /**
     * Close socket, thread pools.
     */
    public static void shutdown() {
        System.out.println("Shutting down...");
        DFS.alive = false;
        try {
            DFS.socket.close();
        } catch (IOException ignore) {}
        DFS.receiver.close();
        DFS.currentNode.close();  // close sender thread pool
    }
}
