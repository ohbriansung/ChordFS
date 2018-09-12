package edu.usfca.cs.dfs;

import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

public class DFS {
    static final int MAX_CHUNK_SIZE = 16 * 1024 * 1024;
    static volatile boolean alive = true;

    private static final int THREAD = 8;

    public static void main(String[] args) {
        Map<String, String> arguments = parseArgs(args);

        /*if (!checkArgs(arguments)) {
            System.out.println("Usage: java dfs.jar --run <storage/client> --port <port> ...");
            System.exit(1);
        }*/

        startReceiver(arguments, THREAD);
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

        return map;
    }

    private static boolean checkArgs(Map<String, String> arguments) {
        boolean checked = true;

        if (!arguments.containsKey("run") || !arguments.containsKey("port")) {
            checked = false;
        }

        return checked;
    }

    private static void startReceiver(Map<String, String> arguments, int thread) {
        // TODO: delete debug mode
        if (arguments.size() == 0) {
            arguments.put("port", "13000");
        }

        int port = Integer.parseInt(arguments.get("port"));

        try {
            Receiver receiver = new Receiver(port, thread);

            Thread listen = new Thread(receiver);
            listen.start();
            listen.join();
        } catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
