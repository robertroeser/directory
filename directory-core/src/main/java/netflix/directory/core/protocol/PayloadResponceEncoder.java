/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class PayloadResponceEncoder
{
    public static final int BLOCK_LENGTH = 8;
    public static final int TEMPLATE_ID = 8;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final PayloadResponceEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    protected int offset;
    protected int limit;
    protected int actingBlockLength;
    protected int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public int offset()
    {
        return offset;
    }

    public PayloadResponceEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);
        return this;
    }

    public int size()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        buffer.checkLimit(limit);
        this.limit = limit;
    }

    public static long transactionIdNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long transactionIdMinValue()
    {
        return 0x0L;
    }

    public static long transactionIdMaxValue()
    {
        return 0xfffffffffffffffeL;
    }
    public PayloadResponceEncoder transactionId(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int binDataEncodingId()
    {
        return 2;
    }

    public static String binDataEncodingCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String binDataEncodingMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public int putBinDataEncoding(final uk.co.real_logic.agrona.DirectBuffer src, final int srcOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        limit(limit + sizeOfLengthField + length);
        CodecUtil.uint8Put(buffer, limit, (short)length);
        buffer.putBytes(limit + sizeOfLengthField, src, srcOffset, length);

        return length;
    }

    public int putBinDataEncoding(final byte[] src, final int srcOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        limit(limit + sizeOfLengthField + length);
        CodecUtil.uint8Put(buffer, limit, (short)length);
        buffer.putBytes(limit + sizeOfLengthField, src, srcOffset, length);

        return length;
    }

    public void binDataEncoding(final String value)
    {
        final byte[] bytes;
        try
        {
            bytes = value.getBytes("UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        final int length = bytes.length;
        final int sizeOfLengthField = 1;
        final int limit = limit();
        limit(limit + sizeOfLengthField + length);
        CodecUtil.uint8Put(buffer, limit, (short)length);
        buffer.putBytes(limit + sizeOfLengthField, bytes, 0, length);
    }
}
