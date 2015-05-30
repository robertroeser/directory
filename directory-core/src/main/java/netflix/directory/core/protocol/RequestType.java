/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

public enum RequestType
{
    GET((short)0),
    PUT((short)1),
    DELETE((short)2),
    NULL_VAL((short)255);

    private final short value;

    RequestType(final short value)
    {
        this.value = value;
    }

    public short value()
    {
        return value;
    }

    public static RequestType get(final short value)
    {
        switch (value)
        {
            case 0: return GET;
            case 1: return PUT;
            case 2: return DELETE;
        }

        if ((short)255 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
