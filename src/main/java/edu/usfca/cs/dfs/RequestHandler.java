package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.cs.dfs.FileTransfer.StorageProcess;
import edu.usfca.cs.dfs.Storage.Node;
import edu.usfca.cs.dfs.Storage.StorageNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;


class RequestHandler extends Serializer implements Runnable {
    private final Socket listening;
    private final String addr;

    RequestHandler(Socket listening) {
        this.listening = listening;
        this.addr = listening.getRemoteSocketAddress().toString();
    }

    @Override
    public void run() {
        try (InputStream in = this.listening.getInputStream()) {
            StorageMessages.Message message = StorageMessages.Message.parseDelimitedFrom(in);
            parseMessage(message);

            this.listening.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseMessage(StorageMessages.Message message) throws IOException {
        StorageMessages.messageType type = message.getType();
        StorageProcess process;

        try {
            switch (type) {
                case INFO:
                    StorageMessages.Info info = StorageMessages.Info.parseFrom(message.getData());
                    System.out.println("Received " + info.getType().name() + " from " + this.addr);
                    parseInfo(info);
                    break;
                case HEARTBEAT:
                    break;  // no action
                case DATA:
                    process = new StorageProcess(message);
                    process.store();
                    ((StorageNode) DFS.currentNode).recordMetadata(message);
                    ((StorageNode) DFS.currentNode).replicate(message);
                    break;
                case REQUEST:
                    process = new StorageProcess(message);
                    StorageMessages.Message data = process.retrieve();
                    response(data);
                    break;
                case FIND_HOST:
                    BigInteger hash = new BigInteger(message.getData().toByteArray());
                    responseNode(((StorageNode) DFS.currentNode).findHost(hash));
                    break;
                case NUM_CHUNKS:
                    System.out.println("Received " + message.getType().name() + " from " + this.addr);
                    int total = ((StorageNode) DFS.currentNode).getTotalChunk(message);
                    response(serializeMessage(total));
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void parseInfo(StorageMessages.Info info) throws IOException {
        StorageMessages.infoType type = info.getType();

        switch (type) {
            case NOTIFY:
                Node n = parseNode(info.getData());
                if (n != null) {
                    ((StorageNode) DFS.currentNode).notify(n);
                }
                break;
            case CLOSEST_PRECEDING_NODE:
                responseNode(((StorageNode) DFS.currentNode).closestPrecedingNode(info.getIntegerData()));
                break;
            case ASK_SUCCESSOR:
                responseNode(((StorageNode) DFS.currentNode).findSuccessor(info.getIntegerData()));
                break;
            case ASK_PREDECESSOR:
                responseNode(((StorageNode) DFS.currentNode).getPredecessor());
                break;
            case ASK_NODE_DETAIL:
                responseNode(((StorageNode) DFS.currentNode).getN());
                break;
            case LIST_NODE:
                response(((StorageNode) DFS.currentNode).listNode(info));
                break;
            case ASK_M:
                responseM(((StorageNode) DFS.currentNode).getM());
        }
    }

    private void responseNode(Node node) throws IOException {
        OutputStream out = this.listening.getOutputStream();
        StorageMessages.Node n = node.serialize();
        n.writeDelimitedTo(out);
    }

    private Node parseNode(ByteString nodeBytes) {
        try {
            StorageMessages.Node node = StorageMessages.Node.parseFrom(nodeBytes);
            return new Node(node);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void responseM(int m) throws IOException {
        OutputStream out = this.listening.getOutputStream();
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.M, m);
        info.writeDelimitedTo(out);
    }

    private void response(StorageMessages.Message data) throws IOException {
        OutputStream out = this.listening.getOutputStream();
        data.writeDelimitedTo(out);
    }

    private void response(StorageMessages.Info info) throws IOException {
        OutputStream out = this.listening.getOutputStream();
        info.writeDelimitedTo(out);
    }
}