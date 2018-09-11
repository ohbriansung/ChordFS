package edu.usfca.cs.dfs;

import java.net.DatagramPacket;

public class RequestHandler implements Runnable {
    private final DatagramPacket packet;

    public RequestHandler(DatagramPacket packet) {
        this.packet = packet;
    }

    @Override
    public void run() {

    }
}