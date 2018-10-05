package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.Sender;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

class Backup extends Sender implements Runnable {
    private final InetSocketAddress addr;
    private final StorageMessages.Message m;
    private final CountDownLatch count;

    Backup(InetSocketAddress addr, StorageMessages.Message m, CountDownLatch count) {
        this.addr = addr;
        this.m = m;
        this.count = count;
    }

    @Override
    public void run() {
        try {
            send(this.addr, this.m);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.count.countDown();
    }
}
