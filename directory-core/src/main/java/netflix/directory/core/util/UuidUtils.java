package netflix.directory.core.util;

import netflix.directory.core.protocol.UuidDecoder;
import netflix.directory.core.protocol.UuidEncoder;

import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public interface UuidUtils {
    default UUID toUUID(UuidDecoder uuidDecoder) {
        long mostSignificationBits = uuidDecoder.mostSignificationBits();
        long leastSignificationBits = uuidDecoder.leastSignificationBits();

        return new UUID(mostSignificationBits, leastSignificationBits);
    }

    default void toUuidEncoder(UuidEncoder encoder, UUID uuid) {
        long mostSignificationBits = uuid.getMostSignificantBits();
        long leastSignificationBits = uuid.getLeastSignificantBits();

        encoder.mostSignificationBits(mostSignificationBits);
        encoder.leastSignificationBits(leastSignificationBits);
    }
}
