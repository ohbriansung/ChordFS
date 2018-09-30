package edu.usfca.cs.dfs.Client;

import edu.usfca.cs.dfs.*;
import edu.usfca.cs.dfs.Storage.Node;
import edu.usfca.cs.dfs.hash.HashException;
import edu.usfca.cs.dfs.hash.SHA1;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;

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
            case "download":
                download(command[1]);
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

    /**
     * Break files into chunks with 64 Mb maximum size.
     * Use SHA1 to hash the filename with chunk id.
     * Create thread for each chunk and send them to the correct storage node.
     * @param filename
     */
    private void upload(String filename) {
        Path path = Paths.get(filename);
        if (filename.contains("/")) {
            // if the file is in other directory, use only the file name without path.
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
            System.out.println("File [" + path + "] does not exist.");
        } catch (HashException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Hash the file name with first chunk id (0) to find a node currently storing it,
     * and get the total number of chunks.
     * Create thread for each chunk and retrieve the chunks from the correct storage node.
     * @param filename
     */
    private void download(String filename) {
        try {
            // get total chunk number
            BigInteger firstHash = this.sha1.hash((filename + 0).getBytes());
            System.out.println("firstHash = [" + firstHash + "]");
            InetSocketAddress firstKeyNode = getRemoteNode(firstHash, this.storageNodeAddress);
            StorageMessages.Message message = serializeMessage(filename, firstHash);
            StorageMessages.Message response = ask(firstKeyNode, message);
            int totalChunk = response.getTotalChunk();

            if (totalChunk == 0) {
                throw new NullPointerException();
            }

            // download each chunk and wait for download to finish
            byte[][] chunks = new byte[totalChunk][DFS.MAX_CHUNK_SIZE];
            CountDownLatch count = new CountDownLatch(totalChunk);
            boolean success = download(filename, chunks, count, firstKeyNode);

            // restore it if download process successes.
            if (success) {
                this.dp.restoreFile(filename, chunks);
                System.out.println("Download process has been completed.");
            }
            else {
                System.out.println("Download process did not finish in time, aborted.");
            }
        } catch (NullPointerException ignore) {
            System.out.println("File [" + filename + "] does not exist in the file system.");
        } catch (HashException | IOException e) {
            e.printStackTrace();
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
