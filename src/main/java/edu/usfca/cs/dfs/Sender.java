package edu.usfca.cs.dfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

class Sender extends Serializer {

    Node ask(InetSocketAddress addr, StorageMessages.infoType type, Integer... id) throws IOException {
        // create socket and stream
        Socket socket = new Socket();
        socket.connect(addr);
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        // send message
        StorageMessages.Info info;
        if (id != null) {
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

    int askM(InetSocketAddress addr) throws IOException {
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

    void notify(InetSocketAddress addr, Node n) throws IOException {
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

    void heartbeat(InetSocketAddress addr) throws IOException {
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
}
