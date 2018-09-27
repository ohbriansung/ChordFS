package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

        try {
            switch (type) {
                case INFO:
                    StorageMessages.Info info = StorageMessages.Info.parseFrom(message.getData());
                    System.out.println("Received " + info.getType().name() + " from " + this.addr);
                    parseInfo(info);
                    break;
                case HEARTBEAT:
                    break;  // no action
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
            case ASK_M:
                responseM(((StorageNode) DFS.currentNode).getM());
                break;
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
}