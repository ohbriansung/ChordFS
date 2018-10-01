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
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author Brian Sung
 */
public class Client extends Command {
    private final SHA1 sha1;
    private final DataProcessor dp;
    private final List<InetSocketAddress> addressBuffer;

    public Client(InetSocketAddress storageNodeAddress) {
        super();
        this.sha1 = new SHA1();
        this.dp = new DataProcessor();
        this.addressBuffer = new ArrayList<>();
        addOneNode(storageNodeAddress);

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
        while ((str = scanner.nextLine()) != null) {
            execute(str);
            printInfo();
        }
    }

    private void execute(String str) {
        String[] command = str.split("\\s+");

        switch (command[0]) {
            case "":
                break;
            case "help":
                help();
                break;
            case "upload":
                if (command.length == 1) {
                    invalid();
                }
                else {
                    upload(command[1]);
                }
                break;
            case "download":
                if (command.length == 1) {
                    invalid();
                }
                else {
                    download(command[1]);
                }
                break;
            case "list":
                list(getOneNode());
                break;
            case "file":
                if (command.length < 3 || !command[1].equals("-l")) {
                    invalid();
                }
                else {
                    file(command[2]);
                }
                break;
            case "exit":
                exit();
                break;
            default:
                invalid();
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
            CountDownLatch count = new CountDownLatch(chunks.size());

            System.out.println("Uploading...");
            boolean success = upload(filename, chunks, hashcode, count);
            System.out.println("Upload process was " + (success ? "" : "in") + "complete.");
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
        int totalChunk = 0;

        try {
            // get total chunk number
            BigInteger firstHash = this.sha1.hash((filename + 0).getBytes());
            System.out.println("firstHash = [" + firstHash + "]");

            InetSocketAddress firstKeyNode = null;
            while (firstKeyNode == null) {
                InetSocketAddress n = getOneNode();
                try {
                    firstKeyNode = getRemoteNode(firstHash, n);
                } catch (IOException ignore) {
                    System.out.println("Node [" + n + "] is unreachable.");
                    removeOneNode(n);
                }
            }

            // contact node responsible for key of first chunk to retrieve the total number of chunk
            StorageMessages.Message message = serializeMessage(filename, firstHash);
            try {
                StorageMessages.Message response = ask(firstKeyNode, message);
                totalChunk = response.getTotalChunk();
                addOneNode(firstKeyNode);
            } catch (IOException ignore) {
                System.out.println("Node [" + firstKeyNode + "] is unreachable, please try to download in a few seconds.");
                return;
            }
        } catch (HashException e) {
            e.printStackTrace();
        }

        if (totalChunk == 0) {
            System.out.println("File [" + filename + "] does not exist in the file system.");
            return;
        }

        try {
            // download each chunk and use count down latch to wait for download to finish
            byte[][] chunks = new byte[totalChunk][DFS.MAX_CHUNK_SIZE];
            CountDownLatch count = new CountDownLatch(totalChunk);
            boolean success = download(filename, chunks, count);

            // restore it if download process successes.
            if (success) {
                this.dp.restoreFile(filename, chunks);
                System.out.println("Download process has been completed.");
            }
            else {
                System.out.println("Download process did not finish in time, aborted.");
            }
        } catch (HashException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Use SHA1 algorithm to hash file name with chunk id.
     * @param filename
     * @param chunks
     * @return List
     * @throws HashException
     */
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

    /**
     * Buffer 3 visited address in case of failure.
     * @param addr
     */
    public void addOneNode(InetSocketAddress addr) {
        synchronized (this.addressBuffer) {
            if (this.addressBuffer.size() < 3 && !this.addressBuffer.contains(addr)) {
                if (this.addressBuffer.add(addr)) {
                    System.out.println("Address Buffer = " + this.addressBuffer.toString());
                }
            }
        }
    }

    /**
     * Remove a node address when it is unreachable.
     * @param addr
     */
    public void removeOneNode(InetSocketAddress addr) {
        synchronized (this.addressBuffer) {
            if (this.addressBuffer.remove(addr)) {
                System.out.println("Address Buffer = " + this.addressBuffer.toString());
            }
        }
    }

    /**
     * Randomly return a node address from address buffer for load balancing.
     * @return InetSocketAddress
     */
    public InetSocketAddress getOneNode() {
        InetSocketAddress addr;

        synchronized (this.addressBuffer) {
            Random r = new Random();
            int i = r.nextInt(this.addressBuffer.size());
            addr = this.addressBuffer.get(i);
        }

        return addr;
    }
}
