/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.DirectBuffer;

public class UuidDecoder
{
    private DirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public UuidDecoder wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 16;
    }

    public static long mostSignificationBitsNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long mostSignificationBitsMinValue()
    {
        return 0x0L;
    }

    public static long mostSignificationBitsMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long mostSignificationBits()
    {
        return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }


    public static long leastSignificationBitsNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long leastSignificationBitsMinValue()
    {
        return 0x0L;
    }

    public static long leastSignificationBitsMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long leastSignificationBits()
    {
        return CodecUtil.uint64Get(buffer, offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

}
