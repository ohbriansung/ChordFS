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
        }
    }

    private void responseNode(Node node, String time) {
        // serialize node
        String host = node.getHost();
        int port = node.getPort();
        int id = node.getId();
        StorageMessages.Node.Builder builder = StorageMessages.Node.newBuilder().setHost(host).setPort(port).setId(id);

        if (node.getSuccessor() != null) {
            builder = builder.setSuccessor(node.getSuccessor());
        }

        if (node.getSuccessorId() != null) {
            builder = builder.setSuccessorId(node.getSuccessorId());
        }

        if (node.getPredecessor() != null) {
            builder = builder.setPredecessor(node.getPredecessor());
        }

        StorageMessages.Node responseNode = builder.build();

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
}