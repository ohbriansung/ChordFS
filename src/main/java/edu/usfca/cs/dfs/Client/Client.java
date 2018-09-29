package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.*;
import edu.usfca.cs.dfs.hash.HashException;
import edu.usfca.cs.dfs.hash.SHA1;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * @author Brian Sung
 */
public class Client extends Command {
    private final SHA1 sha1;
    private final DataProcessor dp;
    private InetSocketAddress storageNodeAddress;

    public Client(InetSocketAddress storageNodeAddress) {
        super();
        this.sha1 = new SHA1();
        this.dp = new DataProcessor();
        this.storageNodeAddress = storageNodeAddress;

        try {
            // wait for receiver and sender
            DFS.READY.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    public void startUI() {
        Scanner scanner = new Scanner(System.in);

        String str;
        printInfo();
        while (!(str = scanner.nextLine()).equals("exit")) {
            execute(str);
            printInfo();
        }

        DFS.receiver.close();
    }

    private void execute(String str) {
        String[] command = str.split("\\s+");

        switch (command[0]) {
            case "help":
                help();
                break;
            case "upload":
                upload(command[1]);
                break;
            case "exit":
                exit();
                break;
            default:
                try {
                    System.out.println("asking " + this.storageNodeAddress);
                    Node p = ask(this.storageNodeAddress, StorageMessages.infoType.ASK_PREDECESSOR);
                    System.out.println(p.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private void upload(String filename) {
        Path path = Paths.get(filename);
        if (filename.contains("/")) {
            filename = filename.substring(filename.lastIndexOf('/') + 1);
        }

        if (Files.isDirectory(path)) {
            System.out.println("The path indicates a directory, please compress it then try again.");
            return;
        }

        try {
            List<byte[]> chunks = this.dp.breakFile(path);
            List<BigInteger> hashcode = hashChunks(filename, chunks);
            upload(filename, chunks, hashcode, this.storageNodeAddress);
            System.out.println("Upload request has been sent.");
        } catch (NoSuchFileException ignore) {
            System.out.println("The file [" + path + "] does not exist.");
        } catch (HashException | IOException e) {
            e.printStackTrace();
        }
    }

    private void download(String fileName, List<byte[]> chunks) {
        try {
            this.dp.restoreFile(fileName, chunks);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private List<BigInteger> hashChunks(String filename, List<byte[]> chunks) throws HashException {
        int size = chunks.size();
        System.out.println("Converted file into [" + size + "] chunks:");

        List<BigInteger> hashcode = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String chunkName = filename + i;
            BigInteger hash = this.sha1.hash(chunkName.getBytes());
            hashcode.add(hash);
            System.out.println("Chunk [" + i + "] = [" + chunkName + "], hash = [" + hash + "]");
        }

        return hashcode;
    }
}
