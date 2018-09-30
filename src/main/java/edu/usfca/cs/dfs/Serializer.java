package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.math.BigInteger;

public abstract class Serializer {

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

    StorageMessages.Message serializeMessage(int totalChunk) {
        return StorageMessages.Message.newBuilder().setTotalChunk(totalChunk).build();
    }

    protected StorageMessages.Message serializeMessage(String filename, BigInteger hash) {
        return StorageMessages.Message.newBuilder().setType(StorageMessages.messageType.NUM_CHUNKS)
                .setFileName(filename).setHash(ByteString.copyFrom(hash.toByteArray())).build();
    }
}
