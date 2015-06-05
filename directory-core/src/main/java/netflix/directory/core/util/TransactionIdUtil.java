package netflix.directory.core.util;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/4/15.
 */
public class TransactionIdUtil {
    private static AtomicLong id = new AtomicLong(0);

    private static Random rnd = new SecureRandom();

    public static long getTransactionId() {
        return id.incrementAndGet();
    }

    public static long getConnectionId() {
        return rnd.nextLong();
    }

}
