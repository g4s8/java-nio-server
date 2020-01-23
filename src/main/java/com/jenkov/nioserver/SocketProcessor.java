package com.jenkov.nioserver;

import com.jenkov.nioserver.http.MessageReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Created by jjenkov on 16-10-2015.
 */
public class SocketProcessor implements Runnable {

    private final Queue<Socket> inboundSocketQueue;

    private final MessageBuffer readMessageBuffer; //todo   Not used now - but perhaps will be later - to check for space in the buffer before reading from sockets

    private final Queue<Message> outboundMessageQueue = new LinkedList<>(); //todo use a better / faster queue.

    private final Map<Long, Socket> socketMap = new HashMap<>();

    private final ByteBuffer readByteBuffer = ByteBuffer.allocate(1024 * 1024);

    private final ByteBuffer writeByteBuffer = ByteBuffer.allocate(1024 * 1024);

    private final Selector readSelector;

    private final Selector writeSelector;

    private final WriteProxy writeProxy;

    private long nextSocketId = 16 * 1024; //start incoming socket ids from 16K - reserve bottom ids for pre-defined sockets (servers).

    private final Set<Socket> emptyToNonEmptySockets = new HashSet<>();

    private final Set<Socket> nonEmptyToEmptySockets = new HashSet<>();

    private final IMessageProcessor messageProcessor;

    public SocketProcessor(
        final Queue<Socket> inboundSocketQueue,
        final MessageBuffer readMessageBuffer,
        final MessageBuffer writeMessageBuffer,
        final IMessageProcessor messageProcessor) throws IOException {
        this.inboundSocketQueue = inboundSocketQueue;
        this.readMessageBuffer = readMessageBuffer;
        //todo   Not used now - but perhaps will be later - to check for space in the buffer before reading from sockets (space for more to write?)
        this.writeProxy = new WriteProxy(writeMessageBuffer, this.outboundMessageQueue);
        this.messageProcessor = messageProcessor;
        this.readSelector = Selector.open();
        this.writeSelector = Selector.open();
    }

    public void run() {
        while (true) {
            try {
                this.executeCycle();
            } catch (final IOException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void executeCycle() throws IOException {
        this.takeNewSockets();
        this.readFromSockets();
        this.writeToSockets();
    }

    public void takeNewSockets() throws IOException {
        Socket newSocket = this.inboundSocketQueue.poll();
        while (newSocket != null) {
            newSocket.socketId = this.nextSocketId++;
            newSocket.socketChannel.configureBlocking(false);
            newSocket.messageReader = new MessageReader(this.readMessageBuffer);
            newSocket.messageWriter = new MessageWriter();
            this.socketMap.put(newSocket.socketId, newSocket);
            final SelectionKey key = newSocket.socketChannel.register(this.readSelector, SelectionKey.OP_READ);
            key.attach(newSocket);
            newSocket = this.inboundSocketQueue.poll();
        }
    }

    public void readFromSockets() throws IOException {
        final int readReady = this.readSelector.selectNow();
        if (readReady > 0) {
            final Set<SelectionKey> selectedKeys = this.readSelector.selectedKeys();
            final Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
            while (keyIterator.hasNext()) {
                final SelectionKey key = keyIterator.next();
                this.readFromSocket(key);
                keyIterator.remove();
            }
            selectedKeys.clear();
        }
    }

    private void readFromSocket(final SelectionKey key) throws IOException {
        final Socket socket = (Socket) key.attachment();
        socket.messageReader.read(socket, this.readByteBuffer);
        final List<Message> fullMessages = socket.messageReader.getMessages();
        if (fullMessages.size() > 0) {
            for (final Message message : fullMessages) {
                message.socketId = socket.socketId;
                this.messageProcessor.process(message, this.writeProxy);  //the message processor will eventually push outgoing messages into an IMessageWriter for this socket.
            }
            fullMessages.clear();
        }
        if (socket.endOfStreamReached) {
            System.out.println("Socket closed: " + socket.socketId);
            this.socketMap.remove(socket.socketId);
            key.attach(null);
            key.cancel();
            key.channel().close();
        }
    }

    public void writeToSockets() throws IOException {
        // Take all new messages from outboundMessageQueue
        this.takeNewOutboundMessages();
        // Cancel all sockets which have no more data to write.
        this.cancelEmptySockets();
        // Register all sockets that *have* data and which are not yet registered.
        this.registerNonEmptySockets();
        // Select from the Selector.
        final int writeReady = this.writeSelector.selectNow();
        if (writeReady > 0) {
            final Set<SelectionKey> selectionKeys = this.writeSelector.selectedKeys();
            final Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
            while (keyIterator.hasNext()) {
                final SelectionKey key = keyIterator.next();
                final Socket socket = (Socket) key.attachment();
                socket.messageWriter.write(socket, this.writeByteBuffer);
                if (socket.messageWriter.isEmpty()) {
                    this.nonEmptyToEmptySockets.add(socket);
                }
                keyIterator.remove();
            }
            selectionKeys.clear();
        }
    }

    private void registerNonEmptySockets() throws ClosedChannelException {
        for (final Socket socket : this.emptyToNonEmptySockets) {
            socket.socketChannel.register(this.writeSelector, SelectionKey.OP_WRITE, socket);
        }
        this.emptyToNonEmptySockets.clear();
    }

    private void cancelEmptySockets() {
        for (final Socket socket : this.nonEmptyToEmptySockets) {
            final SelectionKey key = socket.socketChannel.keyFor(this.writeSelector);
            key.cancel();
        }
        this.nonEmptyToEmptySockets.clear();
    }

    private void takeNewOutboundMessages() {
        Message outMessage = this.outboundMessageQueue.poll();
        while (outMessage != null) {
            final Socket socket = this.socketMap.get(outMessage.socketId);
            if (socket != null) {
                final MessageWriter messageWriter = socket.messageWriter;
                if (messageWriter.isEmpty()) {
                    messageWriter.enqueue(outMessage);
                    this.nonEmptyToEmptySockets.remove(socket);
                    this.emptyToNonEmptySockets.add(socket);    //not necessary if removed from nonEmptyToEmptySockets in prev. statement.
                } else {
                    messageWriter.enqueue(outMessage);
                }
            }
            outMessage = this.outboundMessageQueue.poll();
        }
    }
}
