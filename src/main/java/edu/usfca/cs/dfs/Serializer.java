package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

abstract class Serializer {

    StorageMessages.Info serializeInfo(StorageMessages.infoType type, ByteString data) {
        return StorageMessages.Info.newBuilder().setType(type).setData(data).build();
    }

    StorageMessages.Info serializeInfo(StorageMessages.infoType type, int i) {
        return StorageMessages.Info.newBuilder().setType(type).setIntegerData(i).build();
    }

    StorageMessages.Info serializeInfo(StorageMessages.infoType type) {
        return StorageMessages.Info.newBuilder().setType(type).build();
    }

    StorageMessages.Message serializeMessage(StorageMessages.messageType type, ByteString data) {
        return StorageMessages.Message.newBuilder().setType(type).setData(data).build();
    }

    StorageMessages.Message serializeMessage(StorageMessages.messageType type) {
        return StorageMessages.Message.newBuilder().setType(type).build();
    }
}
