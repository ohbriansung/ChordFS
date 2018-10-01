package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Client.Client;
import edu.usfca.cs.dfs.DFS;
import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

public class Download extends Sender implements Runnable {
    private final String filename;
    private final int i;
    private final BigInteger hash;
    private final byte[][] chunks;
    private final CountDownLatch count;

    public Download(String filename, int i, BigInteger hash, byte[][] chunks, CountDownLatch count) {
        this.filename = filename;
        this.i = i;
        this.hash = hash;
        this.chunks = chunks;
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

        StorageMessages.Message message = serialize(this.filename, this.i, this.hash);
        try {
            _send(remote, message);
            this.count.countDown();
        } catch (IOException ignore) {
            System.out.println("Node [" + remote + "] is unreachable.");
            ((Client) DFS.currentNode).removeOneNode(remote);
        }
    }

    private StorageMessages.Message serialize(String filename, int i, BigInteger hash) {
        return StorageMessages.Message.newBuilder().setType(StorageMessages.messageType.REQUEST)
                .setFileName(filename).setChunkId(i).setHash(ByteString.copyFrom(hash.toByteArray())).build();
    }

    private void _send(InetSocketAddress addr, StorageMessages.Message message) throws IOException {
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        message.writeDelimitedTo(out);
        StorageMessages.Message response = StorageMessages.Message.parseDelimitedFrom(in);
        byte[] chunk = response.getData().toByteArray();
        synchronized (this.chunks) {
            this.chunks[this.i] = chunk;
        }

        ((Client) DFS.currentNode).addOneNode(addr);
        socket.close();
    }
}
