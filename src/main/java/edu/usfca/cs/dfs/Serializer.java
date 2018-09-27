package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.net.InetSocketAddress;

abstract class Serializer {

    StorageMessages.Info serializeInfo(StorageMessages.infoType type, ByteString data) {
        return StorageMessages.Info.newBuilder().setType(type).setData(data).build();
    }

    private StorageMessages.Info serializeInfo(StorageMessages.infoType type, String time) {
        return StorageMessages.Info.newBuilder().setType(type).setTime(time).build();
    }

    private StorageMessages.Info serializeInfo(StorageMessages.infoType type, ByteString data, String time) {
        return StorageMessages.Info.newBuilder().setType(type).setData(data).setTime(time).build();
    }

    private StorageMessages.Info serializeInfo(StorageMessages.infoType type, ByteString data, int i, String time) {
        return StorageMessages.Info.newBuilder().setType(type).setData(data).setIntegerData(i).setTime(time).build();
    }

    StorageMessages.Message serializeMessage(StorageMessages.messageType type, ByteString data) {
        return StorageMessages.Message.newBuilder().setType(type).setData(data).build();
    }

    StorageMessages.Message serializeMessage(StorageMessages.messageType type) {
        return StorageMessages.Message.newBuilder().setType(type).build();
    }

    void createInfoAndSend(InetSocketAddress address, StorageMessages.infoType type, String time, ByteString... b) {
        // serialize info
        StorageMessages.Info info;
        if (b != null && b.length > 0) {
            info = serializeInfo(type, b[0], time);
        }
        else {
            info = serializeInfo(type, time);
        }

        // serialize request with info type
        ByteString data = info.toByteString();
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, data);

        // send message to node
        System.out.println("Sending info type: " + type);
        DFS.sender.send(message, address);
    }

    void createInfoAndSend(InetSocketAddress address, StorageMessages.infoType type, ByteString b, int i, String time) {
        // serialize info
        StorageMessages.Info info = serializeInfo(type, b, i, time);

        // serialize request with info type
        ByteString data = info.toByteString();
        StorageMessages.Message message = serializeMessage(StorageMessages.messageType.INFO, data);

        // send message to node
        System.out.println("Sending info type: " + type);
        DFS.sender.send(message, address);
    }
}
