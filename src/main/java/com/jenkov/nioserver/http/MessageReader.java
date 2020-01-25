package com.jenkov.nioserver.http;

import com.jenkov.nioserver.Message;
import com.jenkov.nioserver.MessageBuffer;
import com.jenkov.nioserver.Socket;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jjenkov on 18-10-2015.
 */
public class MessageReader {

    private final MessageBuffer messageBuffer;

    private final List<Message> completeMessages = new ArrayList<Message>();

    private Message nextMessage;

    public MessageReader(final MessageBuffer readMessageBuffer) {
        this.messageBuffer = readMessageBuffer;
        this.nextMessage = this.messageBuffer.getMessage();
        this.nextMessage.metaData = new HttpHeaders();
    }

    public void read(final Socket socket, final ByteBuffer byteBuffer) throws IOException {
        final int bytesRead = socket.read(byteBuffer);
        byteBuffer.flip();
        if (byteBuffer.remaining() == 0) {
            byteBuffer.clear();
            return;
        }
        this.nextMessage.writeToMessage(byteBuffer);
        final int endIndex = HttpUtil.parseHttpRequest(
            this.nextMessage.sharedArray,
            this.nextMessage.offset,
            this.nextMessage.offset + this.nextMessage.length,
            (HttpHeaders) this.nextMessage.metaData
        );
        if (endIndex != -1) {
            final Message message = this.messageBuffer.getMessage();
            message.metaData = new HttpHeaders();
            message.writePartialMessageToMessage(this.nextMessage, endIndex);
            this.completeMessages.add(this.nextMessage);
            this.nextMessage = message;
        }
        byteBuffer.clear();
    }

    public List<Message> getMessages() {
        return this.completeMessages;
    }
}
