package edu.usfca.cs.dfs;

import java.net.DatagramPacket;

class RequestHandler implements Runnable {
    private final DatagramPacket packet;

    RequestHandler(DatagramPacket packet) {
        this.packet = packet;
    }

    @Override
    public void run() {

    }
}