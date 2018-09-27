package edu.usfca.cs.dfs;

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
class Client extends Asker {
    private final SHA1 sha1;
    private final DataProcessor dp;
    private InetSocketAddress storageNodeAddress;

    Client(InetSocketAddress storageNodeAddress) {
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

    void startUI() {
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
            default:
                System.out.println("asking " + this.storageNodeAddress);
                Node p = askPredecessor(this.storageNodeAddress);
                System.out.println(p.getId());
        }
    }

    private void help() {
        StringBuilder sb = new StringBuilder();

        sb.append("Commands").append(System.lineSeparator());
        sb.append("upload <file_name>\t\tUpload the file to file system.").append(System.lineSeparator());
        sb.append("exit\t\t\tTerminate the program.");

        System.out.println(sb.toString());
    }

    private void printInfo() {
        System.out.print("Please input command or type \"help\": ");
    }

    private void upload(String filename) {
        Path path = Paths.get(filename);

        if (Files.isDirectory(path)) {
            System.out.println("The path indicated a directory, please compress it then try again.");
            return;
        }

        try {
            List<byte[]> chunks = this.dp.breakFile(filename);
            List<BigInteger> hashcode = hashChunks(filename, chunks);
        } catch (NoSuchFileException ignore) {
            System.out.println("The file \"" + filename + "\" does not exist.");
        } catch (HashException | IOException e) {
            e.printStackTrace();
        }

        //TODO: send chunks
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
        for (int i = 1; i < size - 1; i++) {
            String chunkName = filename + i;
            BigInteger hash = this.sha1.hash(chunkName.getBytes());
            hashcode.add(hash);
            System.out.println("Chunk [" + i + "] name = [" + chunkName + "], hash = [" + hash + "]");
        }

        return hashcode;
    }
}
