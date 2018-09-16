package edu.usfca.cs.dfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;

class RequestHandler implements Runnable {
    private final DatagramPacket packet;

    RequestHandler(DatagramPacket packet) {
        this.packet = packet;
    }

    @Override
    public void run() {
        byte[] receivedData = this.packet.getData();

        try (ByteArrayInputStream inStream = new ByteArrayInputStream(receivedData)) {
            StorageMessages.Info info = StorageMessages.Info.parseDelimitedFrom(inStream);
            System.out.println(info.getType().toString());
            System.out.println(info.getData().toStringUtf8());
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}