package edu.usfca.cs.dfs.FileTransfer;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Node;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Upload implements Runnable {
    private final String filename;
    private final int totalSize;
    private final int i;
    private final byte[] chunk;
    private final BigInteger hash;
    private InetSocketAddress addr;

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
            StorageMessages.Message message = serialize(this.filename, this.totalSize, this.i, this.chunk);
            send(remote, message);
        } catch (IOException e) {
            System.out.println("Failed to upload chunk [" + this.i + "] of file [" + this.filename + "].");
            e.printStackTrace();
        }
    }

    private InetSocketAddress getRemoteNode(BigInteger hash, InetSocketAddress addr) throws IOException {
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        ByteString b = ByteString.copyFrom(hash.toByteArray());
        StorageMessages.Message message = StorageMessages.Message.newBuilder()
                .setType(StorageMessages.messageType.FIND_HOST).setData(b).build();
        message.writeDelimitedTo(out);

        StorageMessages.Node node = StorageMessages.Node.parseFrom(in);
        Node n = new Node(node);
        socket.close();

        return n.getAddress();
    }

    private StorageMessages.Message serialize(String filename, int total, int i, byte[] chunk) {
        StorageMessages.Message message = StorageMessages.Message.newBuilder()
                .setType(StorageMessages.messageType.DATA).setFileName(filename).setTotalChunk(total)
                .setChunkId(i).setData(ByteString.copyFrom(chunk)).build();
        return message;
    }

    private void send(InetSocketAddress addr, StorageMessages.Message message) throws IOException {
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        message.writeDelimitedTo(out);
        socket.close();
    }
}
