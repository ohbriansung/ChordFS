package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.FileTransfer.Download;
import edu.usfca.cs.dfs.FileTransfer.Upload;
import edu.usfca.cs.dfs.Storage.Node;
import edu.usfca.cs.dfs.hash.HashException;
import edu.usfca.cs.dfs.hash.SHA1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class Sender extends Serializer {
    private final ExecutorService pool;

    public Sender() {
        this.pool = Executors.newFixedThreadPool(DFS.THREAD);
    }

    protected void close() {
        if (this.pool != null && !this.pool.isShutdown()) {
            this.pool.shutdown();
        }
    }

    protected Node ask(InetSocketAddress addr, StorageMessages.infoType type, Integer... id) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        // send message
        StorageMessages.Info info;
        if (id != null && id.length > 0) {
            info = serializeInfo(type, id[0]);
        }
        else {
            info = serializeInfo(type);
        }
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, info.toByteString());
        message.writeDelimitedTo(out);

        // receive response and close socket
        StorageMessages.Node n = StorageMessages.Node.parseDelimitedFrom(in);
        socket.close();

        return new Node(n);
    }

    protected StorageMessages.Message ask(InetSocketAddress addr, StorageMessages.infoType type, ByteString data) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        // send message
        StorageMessages.Info info = serializeInfo(type, data);
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, info.toByteString());
        message.writeDelimitedTo(out);

        // receive response and close socket
        StorageMessages.Message m = StorageMessages.Message.parseDelimitedFrom(in);
        socket.close();

        return m;
    }

    protected int askM(InetSocketAddress addr) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        // send message
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.ASK_M);
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, info.toByteString());
        message.writeDelimitedTo(out);

        // receive response and close socket
        StorageMessages.Info response = StorageMessages.Info.parseDelimitedFrom(in);
        socket.close();

        return response.getIntegerData();
    }

    protected void notify(InetSocketAddress addr, Node n) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();

        // send message
        StorageMessages.Node node = n.serialize();
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.NOTIFY, node.toByteString());
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, info.toByteString());
        message.writeDelimitedTo(out);

        // close socket
        socket.close();
    }

    protected void heartbeat(InetSocketAddress addr) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();

        // send message
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.HEARTBEAT);
        message.writeDelimitedTo(out);

        // close socket
        socket.close();
    }

    protected void upload(String filename, List<byte[]> chunks, List<BigInteger> hashcode, InetSocketAddress addr) {
        int size = chunks.size();

        // a thread for a chunk
        for (int i = 0; i < size; i++) {
            this.pool.submit(new Upload(filename, size, i, chunks.get(i), hashcode.get(i), addr));
        }
    }

    protected boolean download(String filename, byte[][] chunks, CountDownLatch count, InetSocketAddress addr) throws HashException {
        SHA1 sha1 = new SHA1();
        int size = chunks.length;

        for (int i = 0; i < size; i++) {
            BigInteger hash = sha1.hash((filename + i).getBytes());
            this.pool.submit(new Download(filename, i, hash, chunks, count, addr));
        }

        boolean success = false;
        try {
            // wait for at most (30 * size) seconds
            success = count.await(30 * size, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return success;
    }

    protected InetSocketAddress getRemoteNode(BigInteger hash, InetSocketAddress addr) throws IOException {
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        ByteString b = ByteString.copyFrom(hash.toByteArray());
        StorageMessages.Message message = StorageMessages.Message.newBuilder()
                .setType(StorageMessages.messageType.FIND_HOST).setData(b).build();
        message.writeDelimitedTo(out);

        StorageMessages.Node node = StorageMessages.Node.parseDelimitedFrom(in);
        Node n = new Node(node);
        socket.close();

        return n.getAddress();
    }
}
