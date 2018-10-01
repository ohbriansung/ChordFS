package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Client.Client;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class Upload extends Sender implements Runnable {
    private final String filename;
    private final int totalSize;
    private final int i;
    private final byte[] chunk;
    private final BigInteger hash;
    private final CountDownLatch count;

    public Upload(String filename, int totalSize, int i, byte[] chunk, BigInteger hash, CountDownLatch count) {
        this.filename = filename;
        this.totalSize = totalSize;
        this.i = i;
        this.chunk = chunk;
        this.hash = hash;
        this.count = count;
    }

    @Override
    public void run() {
        InetSocketAddress remote = null;

        while (remote == null) {
            InetSocketAddress n = ((Client) DFS.currentNode).getOneNode();

            try {
                remote = getRemoteNode(this.hash, n);
            } catch (IOException ignore) {
                System.out.println("Node [" + n + "] is unreachable.");
                ((Client) DFS.currentNode).removeOneNode(n);
            }
        }

        StorageMessages.Message message = serialize(this.filename, this.totalSize, this.i, this.chunk, this.hash);
        try {
            send(remote, message);
            this.count.countDown();
            ((Client) DFS.currentNode).addOneNode(remote);
        } catch (IOException ignore) {
            System.out.println("Failed to upload chunk [" + this.i + "] of file [" + this.filename + "].");
            System.out.println("Node [" + remote + "] is unreachable.");
            ((Client) DFS.currentNode).removeOneNode(remote);
        }
    }

    private StorageMessages.Message serialize(String filename, int total, int i, byte[] chunk, BigInteger hash) {
        return StorageMessages.Message.newBuilder().setType(StorageMessages.messageType.DATA)
                .setFileName(filename).setTotalChunk(total).setChunkId(i).setData(ByteString.copyFrom(chunk))
                .setHash(ByteString.copyFrom(hash.toByteArray())).build();
    }
}
