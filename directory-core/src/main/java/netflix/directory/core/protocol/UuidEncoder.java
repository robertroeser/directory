/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class UuidEncoder
{
    private MutableDirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public UuidEncoder wrap(final MutableDirectBuffer buffer, final int offset, final int actingVersion)
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
    public UuidEncoder mostSignificationBits(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
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
    public UuidEncoder leastSignificationBits(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 8, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}
