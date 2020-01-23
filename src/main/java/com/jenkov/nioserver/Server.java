package com.jenkov.nioserver;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by jjenkov on 24-10-2015.
 */
public class Server {

    private final SocketAccepter socketAccepter;

    private final SocketProcessor socketProcessor;

    public Server(final int port, final IMessageProcessor messageProcessor) throws IOException {
        final Queue<Socket> queue = new ArrayBlockingQueue<>(1024);
        this.socketProcessor = new SocketProcessor(
            queue, new MessageBuffer(), new MessageBuffer(),
            messageProcessor
        );
        this.socketAccepter = new SocketAccepter(port, queue);
    }

    public void start() {
        new Thread(this.socketAccepter).start();
        new Thread(this.socketProcessor).start();
    }
}
