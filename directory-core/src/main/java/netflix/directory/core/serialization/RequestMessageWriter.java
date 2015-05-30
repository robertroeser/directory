package netflix.directory.core.serialization;

import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.RequestMessageEncoder;
import netflix.directory.core.protocol.RequestType;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 5/28/15.
 */
public class RequestMessageWriter {
    private static ThreadLocal<RequestMessageWriter> threadInstance
        = ThreadLocal.withInitial(RequestMessageWriter::new);

    public static RequestMessageWriter getInstance() {
        return threadInstance.get();
    }

    private final MessageHeaderEncoder MESSAGE_HEADER = new MessageHeaderEncoder();
    private final RequestMessageEncoder requestMessageEncoder = new RequestMessageEncoder();

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

    final short messageTemplateVersion = 0;
    int bufferOffset = 0;
    int encodingLength = 0;

    public int encode(RequestType type, String key, String value) {
        return encode(type, key, value, "");
    }

    public int encode(RequestType type, String key, String value, String responseChannel) {
        bufferOffset = 0;
        encodingLength = 0;
        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion)
            .blockLength(requestMessageEncoder.sbeBlockLength())
            .templateId(requestMessageEncoder.sbeTemplateId())
            .schemaId(requestMessageEncoder.sbeSchemaId())
            .version(requestMessageEncoder.sbeSchemaVersion());

        bufferOffset += MESSAGE_HEADER.size();
        encodingLength += MESSAGE_HEADER.size();

        requestMessageEncoder.wrap(directBuffer, bufferOffset);

        requestMessageEncoder.type(type);
        requestMessageEncoder.key(key);
        requestMessageEncoder.value(value);
        requestMessageEncoder.responseChannel(responseChannel);

        encodingLength += requestMessageEncoder.size();
        return encodingLength;
    }

    // getting bytes via copy ... figure out way to do this without copy
    public byte[] getBytes() {
        byte[] theBytes = new byte[encodingLength];
        directBuffer.getBytes(0, theBytes, 0, encodingLength);
        return theBytes;
    }

    public RequestMessageEncoder getRequestMessageEncoder() {
        return requestMessageEncoder;
    }

    public MessageHeaderEncoder getMESSAGE_HEADER() {
        return MESSAGE_HEADER;
    }
}
