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

        switch (type) {
            case CLOSEST_PRECEDING_FINGER:
                int id = Integer.parseInt(info.getData().toStringUtf8());
                Node closest = DFS.storageNode.closestPrecedingFinger(id);
                responseNode(closest, info.getTime());
                break;
            case NODE:
                Node node = parseNode(info.getData());
                DFS.storageNode.addAnswer(info.getTime(), node);
                DFS.storageNode.awaitTasksCountDown(info.getTime());
                break;
        }
    }

    private void responseNode(Node node, long time) {
        // serialize node
        String host = node.getHost();
        int port = node.getPort();
        int id = node.getId();
        String successor = node.getSuccessor();
        int successorId = node.getSuccessorId();
        String predecessor = node.getPredecessor();
        StorageMessages.Node responseNode = StorageMessages.Node.newBuilder().setHost(host).setPort(port).setId(id)
                .setSuccessor(successor).setSuccessorId(successorId).setPredecessor(predecessor).build();

        // serialize info
        ByteString nodeBytes = responseNode.toByteString();
        StorageMessages.Info info = serializeInfo(StorageMessages.infoType.NODE, nodeBytes, time);

        // serialize request
        ByteString data = info.toByteString();
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, data);

        // send response to node
        DFS.sender.send(message, this.remoteAddress);
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
}