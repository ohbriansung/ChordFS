package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
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
    private final InetSocketAddress addr;

    public Download(String filename, int i, BigInteger hash, byte[][] chunks, CountDownLatch count, InetSocketAddress addr) {
        this.filename = filename;
        this.i = i;
        this.hash = hash;
        this.chunks = chunks;
        this.count = count;
        this.addr = addr;
    }

    @Override
    public void run() {
        try {
            InetSocketAddress remote = getRemoteNode(this.hash, this.addr);
            StorageMessages.Message message = serialize(this.filename, this.i, this.hash);
            _send(remote, message);
            this.count.countDown();
        } catch (IOException e) {
            e.printStackTrace();
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

        socket.close();
    }
}
