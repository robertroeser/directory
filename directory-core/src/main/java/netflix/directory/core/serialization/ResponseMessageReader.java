package netflix.directory.core.serialization;

import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.ResponseMessageDecoder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Created by rroeser on 5/28/15.
 */
public class ResponseMessageReader {
    private static ThreadLocal<ResponseMessageReader> threadInstance
        = ThreadLocal.withInitial(ResponseMessageReader::new);

    public static ResponseMessageReader getInstance() {
        return threadInstance.get();
    }

    public static ResponseMessageDecoder wrap(byte[] bytes) {
        return threadInstance.get().decode(bytes);
    }

    final MessageHeaderDecoder MESSAGE_HEADER = new MessageHeaderDecoder();
    final ResponseMessageDecoder responseMessageDecoder = new ResponseMessageDecoder();

    final UnsafeBuffer directBuffer = new UnsafeBuffer(new byte[0]);

    final short messageTemplateVersion = 0;
    int bufferOffset = 0;
    byte[] originalBytes = null;


    public ResponseMessageDecoder decode(byte[] bytes) {
        originalBytes = bytes;
        bufferOffset = 0;
        directBuffer.wrap(bytes);
        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion);

        final int actingBlockLength = MESSAGE_HEADER.blockLength();
        final int actingVersion = MESSAGE_HEADER.version();

        bufferOffset += MESSAGE_HEADER.size();

        responseMessageDecoder.wrap(directBuffer, bufferOffset, actingBlockLength, actingVersion);

        return responseMessageDecoder;
    }

    public byte[] getOriginalBytes() {
        return originalBytes;
    }

    public MessageHeaderDecoder getMESSAGE_HEADER() {
        return MESSAGE_HEADER;
    }

    public ResponseMessageDecoder getResponseMessageDecoder() {
        return responseMessageDecoder;
    }
}
