package edu.usfca.cs.dfs;

import com.sun.javafx.binding.StringFormatter;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class DFS {
    static final int MAX_CHUNK_SIZE = 64 * 1024 * 1024;
    static final int THREAD = 16;
    static final CountDownLatch READY = new CountDownLatch(1);  // ui waits for receiver and sender
    static volatile boolean alive = true;

    static ServerSocket socket;
    static Receiver receiver;
    static Sender currentNode;
    static String volume;

    public static void main(String[] args) {
        Map<String, String> arguments = parseArgs(args);

        // TODO: uncomment
        /*if (!checkArgs(arguments)) {
            System.out.println("Usage: java dfs.jar --run <storage/client> ...");
            System.exit(1);
        }*/

        // default arguments
        String host = "";
        int port = 13000;
        int m = 4;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
            port = (arguments.containsKey("port") ? Integer.parseInt(arguments.get("port")) : port);
            m = (arguments.containsKey("m") ? Integer.parseInt(arguments.get("m")) : m);
            DFS.socket = new ServerSocket(port);
        } catch (NumberFormatException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        DFS.receiver = startReceiver(host, port);
        startNode(arguments, host, port, m);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < args.length - 1; i += 2) {
            String key;

            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            }
            else {
                continue;
            }

            map.put(key, args[i + 1]);
        }

        map.put("run", "storage");
        map.put("port", "13000");
        //map.put("node", "localhost:13000");

//        map.put("run", "client");
//        map.put("port", "13099");
//        map.put("node", "localhost:13000");

        return map;
    }

    private static boolean checkArgs(Map<String, String> arguments) {
        boolean checked = true;

        if (!arguments.containsKey("run")) {
            checked = false;
        }

        return checked;
    }

    private static Receiver startReceiver(String host, int port) {
        Receiver receiver = new Receiver(host, port);
        Thread listen = new Thread(receiver);
        listen.start();

        return receiver;
    }

    private static void startNode(Map<String, String> arguments, String host, int port, int m) {
        if (arguments.get("run").equals("client")) {
            String[] address = arguments.get("node").split(":");
            DFS.currentNode = new Client(new InetSocketAddress(address[0], Integer.parseInt(address[1])));
            ((Client) DFS.currentNode).startUI();
        }
        else {
            DFS.volume = arguments.getOrDefault("volume", "~/Downloads/");

            if (arguments.containsKey("node")) {
                DFS.currentNode = new StorageNode(host, port);

                String[] address = arguments.get("node").split(":");
                InetSocketAddress np = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
                try {
                    ((StorageNode) DFS.currentNode).join(np);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Node " + np.toString() + "is unreachable, please try again.");
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
}
