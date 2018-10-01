package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Client.Client;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;

public class Upload extends Sender implements Runnable {
    private final String filename;
    private final int totalSize;
    private final int i;
    private final byte[] chunk;
    private final BigInteger hash;
    private final InetSocketAddress addr;

    public Upload(String filename, int totalSize, int i, byte[] chunk, BigInteger hash, InetSocketAddress addr) {
        this.filename = filename;
        this.totalSize = totalSize;
        this.i = i;
        this.chunk = chunk;
        this.hash = hash;
        this.addr = addr;
    }

    @Override
    public void run() {
        try {
            InetSocketAddress remote = getRemoteNode(this.hash, this.addr);
            StorageMessages.Message message = serialize(this.filename, this.totalSize, this.i, this.chunk, this.hash);
            send(remote, message);
            ((Client) DFS.currentNode).addOneNode(remote);
        } catch (IOException e) {
            System.out.println("Failed to upload chunk [" + this.i + "] of file [" + this.filename + "].");
            e.printStackTrace();
        }
    }

    private StorageMessages.Message serialize(String filename, int total, int i, byte[] chunk, BigInteger hash) {
        return StorageMessages.Message.newBuilder().setType(StorageMessages.messageType.DATA)
                .setFileName(filename).setTotalChunk(total).setChunkId(i).setData(ByteString.copyFrom(chunk))
                .setHash(ByteString.copyFrom(hash.toByteArray())).build();
    }
}
