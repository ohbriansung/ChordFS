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

            System.out.println("Received request: " + request.getType().name());
            parseRequest(request);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void parseRequest(StorageMessages.Message request) {
        StorageMessages.messageType type = request.getType();

        try {
            if (type == StorageMessages.messageType.INFO) {
                StorageMessages.Info info = StorageMessages.Info.parseFrom(request.getData());

                System.out.println("Received info: " + info.getType().name());
                parseInfo(info);
            }
            else if (type == StorageMessages.messageType.ACK) {
                DFS.storageNode.awaitTasksCountDown(request.getData().toStringUtf8());
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
                responseNode(DFS.storageNode.closestPrecedingFinger(id), info.getTime());
                break;
            case NODE:
                DFS.storageNode.addAnswer(info.getTime(), parseNode(info.getData()));
                DFS.storageNode.awaitTasksCountDown(info.getTime());
                break;
            case ASK_M:
                responseM(DFS.storageNode.getM(), info.getTime());
                break;
            case M:
                DFS.storageNode.addAnswer(info.getTime(), Integer.parseInt(info.getData().toStringUtf8()));
                DFS.storageNode.awaitTasksCountDown(info.getTime());
                break;
            case ASK_SUCCESSOR:
                id = Integer.parseInt(info.getData().toStringUtf8());
                responseNode(DFS.storageNode.findSuccessor(id), info.getTime());
                break;
            case ASK_NODE_DETAIL:
                responseNode(DFS.storageNode.getSelf(), info.getTime());
                break;
            case UPDATE_PREDECESSOR:
                Node predecessor = parseNode(info.getData());
                if (predecessor != null) {
                    DFS.storageNode.getSelf().setPredecessor(predecessor.getAddress());
                    DFS.storageNode.selfUpdate(predecessor);
                    ack(info.getTime());
                }
                break;
            case UPDATE_FINGER_TABLE:
                Node s = parseNode(info.getData());
                DFS.storageNode.updateFingerTable(s, info.getIntegerData());
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