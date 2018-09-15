package edu.usfca.cs.dfs;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class DFS {
    static final int MAX_CHUNK_SIZE = 16 * 1024 * 1024;
    static final int THREAD = 8;
    static final CountDownLatch READY = new CountDownLatch(1);  // ui waits for receiver and sender
    static DatagramSocket SOCKET;
    static volatile boolean alive = true;

    public static void main(String[] args) {
        Map<String, String> arguments = parseArgs(args);

        // TODO: uncomment
        /*if (!checkArgs(arguments)) {
            System.out.println("Usage: java dfs.jar --run <storage/client> ...");
            System.exit(1);
        }*/

        try {
            SOCKET = new DatagramSocket(Integer.parseInt(arguments.get("port")));
        } catch (SocketException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Receiver receiver = startReceiver();
        Sender sender = new Sender();

        if (arguments.get("run").equals("client")) {
            UserInterface ui = new UserInterface(receiver, sender);
            ui.start();
        }
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

        map.put("run", "client");
        map.put("port", "13000");

        return map;
    }

    private static boolean checkArgs(Map<String, String> arguments) {
        boolean checked = true;

        if (!arguments.containsKey("run")) {
            checked = false;
        }

        return checked;
    }

    private static Receiver startReceiver() {
        Receiver receiver = new Receiver();
        Thread listen = new Thread(receiver);
        listen.start();

        return receiver;
    }
}
