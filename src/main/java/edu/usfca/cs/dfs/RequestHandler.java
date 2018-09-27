package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;


class RequestHandler extends Serializer implements Runnable {
    private final DatagramPacket packet;
    private final InetSocketAddress remoteAddress;

    RequestHandler(DatagramPacket packet) {
        this.packet = packet;
        this.remoteAddress = new InetSocketAddress(packet.getAddress(), packet.getPort());
    }

    @Override
    public void run() {
        byte[] receivedData = this.packet.getData();

        try (ByteArrayInputStream inStream = new ByteArrayInputStream(receivedData)) {
            StorageMessages.Message request = StorageMessages.Message.parseDelimitedFrom(inStream);

            parseMessage(request);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void parseMessage(StorageMessages.Message message) {
        StorageMessages.messageType type = message.getType();

        try {
            switch (type) {
                case INFO:
                    StorageMessages.Info info = StorageMessages.Info.parseFrom(message.getData());
                    System.out.println("Received " + info.getType().name() + " from " + this.remoteAddress);
                    parseInfo(info);
                    break;
                case ACK:
                    System.out.println("Received ack from " + this.remoteAddress);
                    DFS.currentNode.awaitTasksCountDown(message.getData().toStringUtf8());
                    break;
                case HEARTBEAT:
                    break;  // no action
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void parseInfo(StorageMessages.Info info) {
        StorageMessages.infoType type = info.getType();
        int id;

        switch (type) {
            case CLOSEST_PRECEDING_FINGER:
                id = Integer.parseInt(info.getData().toStringUtf8());
                responseNode(DFS.currentNode.closestPrecedingNode(id), info.getTime());
                break;
            case NODE:
                DFS.currentNode.addAnswer(info.getTime(), parseNode(info.getData()));
                DFS.currentNode.awaitTasksCountDown(info.getTime());
                break;
            case ASK_M:
                responseM(DFS.currentNode.getM(), info.getTime());
                break;
            case M:
                DFS.currentNode.addAnswer(info.getTime(), Integer.parseInt(info.getData().toStringUtf8()));
                DFS.currentNode.awaitTasksCountDown(info.getTime());
                break;
            case ASK_SUCCESSOR:
                id = Integer.parseInt(info.getData().toStringUtf8());
                responseNode(DFS.currentNode.findSuccessor(id), info.getTime());
                break;
            case ASK_PREDECESSOR:
                responseNode(DFS.currentNode.getPredecessor(), info.getTime());
                break;
            case ASK_NODE_DETAIL:
                responseNode(DFS.currentNode.getN(), info.getTime());
                break;
            case NOTIFY:
                DFS.currentNode.notify(parseNode(info.getData()));
                ack(info.getTime());
                break;
        }
    }

    private void responseNode(Node node, String time) {
        StorageMessages.Node responseNode = node.serialize();
        createInfoAndSend(this.remoteAddress, StorageMessages.infoType.NODE, time, responseNode.toByteString());
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

    private void responseM(int m, String time) {
        createInfoAndSend(this.remoteAddress, StorageMessages.infoType.M,
                time, ByteString.copyFromUtf8(String.valueOf(m)));
    }

    // TODO: temp ack
    private void ack(String time) {
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.ACK, ByteString.copyFromUtf8(time));
        DFS.sender.send(message, this.remoteAddress);
    }
}