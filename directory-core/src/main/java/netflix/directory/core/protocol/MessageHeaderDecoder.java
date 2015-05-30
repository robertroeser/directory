/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.DirectBuffer;

public class MessageHeaderDecoder
{
    private DirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public MessageHeaderDecoder wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 8;
    }

    public static int blockLengthNullValue()
    {
        return 65535;
    }

    public static int blockLengthMinValue()
    {
        return 0;
    }

    public static int blockLengthMaxValue()
    {
        return 65534;
    }

    public int blockLength()
    {
        return CodecUtil.uint16Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int templateIdNullValue()
    {
        return 65535;
    }

    public static int templateIdMinValue()
    {
        return 0;
    }

    public static int templateIdMaxValue()
    {
        return 65534;
    }

    public int templateId()
    {
        return CodecUtil.uint16Get(buffer, offset + 2, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int schemaIdNullValue()
    {
        return 65535;
    }

    public static int schemaIdMinValue()
    {
        return 0;
    }

    public static int schemaIdMaxValue()
    {
        return 65534;
    }

    public int schemaId()
    {
        return CodecUtil.uint16Get(buffer, offset + 4, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static int versionNullValue()
    {
        return 65535;
    }

    public static int versionMinValue()
    {
        return 0;
    }

    public static int versionMaxValue()
    {
        return 65534;
    }

    public int version()
    {
        return CodecUtil.uint16Get(buffer, offset + 6, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

}
