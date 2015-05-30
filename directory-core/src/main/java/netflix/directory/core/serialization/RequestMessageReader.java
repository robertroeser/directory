package netflix.directory.core.serialization;

import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.RequestMessageDecoder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Created by rroeser on 5/28/15.
 */
public class RequestMessageReader {
    private static ThreadLocal<RequestMessageReader> threadInstance
        = ThreadLocal.withInitial(RequestMessageReader::new);

    public static RequestMessageReader getInstance() {
        return threadInstance.get();
    }
    public static RequestMessageDecoder wrap(byte[] bytes) {
        return threadInstance.get().decode(bytes);
    }

    final MessageHeaderDecoder MESSAGE_HEADER = new MessageHeaderDecoder();
    final RequestMessageDecoder requestMessageDecoder = new RequestMessageDecoder();

    final UnsafeBuffer directBuffer = new UnsafeBuffer(new byte[0]);

    final short messageTemplateVersion = 0;
    int bufferOffset = 0;
    byte[] originalBytes = null;

    public RequestMessageDecoder decode(byte[] bytes) {
        originalBytes = bytes;
        bufferOffset = 0;
        directBuffer.wrap(bytes);
        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion);

        final int actingBlockLength = MESSAGE_HEADER.blockLength();
        final int actingVersion = MESSAGE_HEADER.version();

        bufferOffset += MESSAGE_HEADER.size();

        requestMessageDecoder.wrap(directBuffer, bufferOffset, actingBlockLength, actingVersion);

        return requestMessageDecoder;
    }

    public byte[] getOriginalBytes() {
        return originalBytes;
    }

    public RequestMessageDecoder getRequestMessageDecoder() {
        return requestMessageDecoder;
    }

    public MessageHeaderDecoder getMESSAGE_HEADER() {
        return MESSAGE_HEADER;
    }
}
