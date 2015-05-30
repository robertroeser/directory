/* Generated SBE (Simple Binary Encoding) message codec */
package netflix.directory.core.protocol;

public enum ResponseCode
{
    SUCCESS((short)0),
    ERROR((short)1),
    NULL_VAL((short)255);

    private final short value;

    ResponseCode(final short value)
    {
        this.value = value;
    }

    public short value()
    {
        return value;
    }

    public static ResponseCode get(final short value)
    {
        switch (value)
        {
            case 0: return SUCCESS;
            case 1: return ERROR;
        }

        if ((short)255 == value)
        {
            return NULL_VAL;
        }

        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
