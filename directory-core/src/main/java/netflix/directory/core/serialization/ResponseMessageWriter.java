package netflix.directory.core.serialization;

import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.ResponseCode;
import netflix.directory.core.protocol.ResponseMessageEncoder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 5/28/15.
 */
public class ResponseMessageWriter {
    private static ThreadLocal<ResponseMessageWriter> threadInstance
        = ThreadLocal.withInitial(ResponseMessageWriter::new);

    public static ResponseMessageWriter getInstance() {
        return threadInstance.get();
    }

    private final MessageHeaderEncoder MESSAGE_HEADER = new MessageHeaderEncoder();
    private final ResponseMessageEncoder responseMessageEncoder = new ResponseMessageEncoder();

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

    final short messageTemplateVersion = 0;
    int bufferOffset = 0;
    int encodingLength = 0;

    public int encode(ResponseCode responseCode, String value) {
        return encode(responseCode, "", value);
    }

    public int encode(ResponseCode responseCode, String key, String value) {
        bufferOffset = 0;
        encodingLength = 0;
        MESSAGE_HEADER.wrap(directBuffer, bufferOffset, messageTemplateVersion)
            .blockLength(responseMessageEncoder.sbeBlockLength())
            .templateId(responseMessageEncoder.sbeTemplateId())
            .schemaId(responseMessageEncoder.sbeSchemaId())
            .version(responseMessageEncoder.sbeSchemaVersion());

        bufferOffset += MESSAGE_HEADER.size();
        encodingLength += MESSAGE_HEADER.size();

        responseMessageEncoder.wrap(directBuffer, bufferOffset);

        responseMessageEncoder.code(responseCode);
        responseMessageEncoder.value(value);
        responseMessageEncoder.key(key);

        encodingLength += responseMessageEncoder.size();
        return encodingLength;
    }

    // getting bytes via copy ... figure out way to do this without copy
    public byte[] getBytes() {
        byte[] theBytes = new byte[encodingLength];
        directBuffer.getBytes(0, theBytes, 0, encodingLength);
        return theBytes;
    }

    public ResponseMessageEncoder getResponseMessageEncoder() {
        return responseMessageEncoder;
    }

    public MessageHeaderEncoder getMESSAGE_HEADER() {
        return MESSAGE_HEADER;
    }
}
