package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

abstract class Serializer {

    StorageMessages.Info serializeInfo(StorageMessages.infoType type, ByteString data, long time) {
        return StorageMessages.Info.newBuilder().setType(type).setData(data).setTime(time).build();
    }

    StorageMessages.Message serializeMessage(StorageMessages.messageType type, ByteString data) {
        return StorageMessages.Message.newBuilder().setType(type).setData(data).build();
    }
}
