package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.hash.SHA1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

class Client extends Asker {
    private final SHA1 sha1;
    private InetSocketAddress storageNodeAddress;

    Client(InetSocketAddress storageNodeAddress) {
        super();
        this.sha1 = new SHA1();
        this.storageNodeAddress = storageNodeAddress;

        try {
            // wait for receiver and sender
            DFS.READY.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    void startUI() {
        Scanner scanner = new Scanner(System.in);

        String command;
        printInfo();
        while (!(command = scanner.nextLine()).equals("exit")) {
            System.out.println("asking " + this.storageNodeAddress);
            Node p = askPredecessor(this.storageNodeAddress);
            System.out.println(p.getId());

            printInfo();
        }

        DFS.receiver.close();
    }

    private void printInfo() {
        System.out.print("Please input command or type \"help\": ");
    }

    public void upload(String fileName) throws IOException {
        Path path = Paths.get(fileName);

        if (Files.isDirectory(path)) {
            System.out.println("The path indicated a directory, please compress it then try again.");
            return;
        }

        DataProcessor dp = new DataProcessor();
        List<byte[]> chunks = dp.breakFile(fileName);

        //TODO: send chunks
    }

    public void download(String fileName, List<byte[]> chunks) {
        DataProcessor dp = new DataProcessor();

        try {
            dp.restoreFile(fileName, chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // TODO: delete debug
    public static void main(String[] args) {
        DataProcessor dp = new DataProcessor();

        try {
            List<byte[]> chunks = dp.breakFile("/Users/brian/Downloads/Cell_Phones_and_Accessories_5.json");
            System.out.println(chunks.size());
            dp.restoreFile("/Users/brian/Downloads/Cell_Phones_and_Accessories_5_copy.json", chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
