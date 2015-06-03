package netflix.directory.core.ctx;

import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public class RequestContext {

    private String key;
    private String address;
    private UUID transactionId;


    public RequestContext(String key, String address, UUID transactionId) {
        this.key = key;
        this.address = address;
        this.transactionId = transactionId;
    }

    public String getKey() {
        return key;
    }

    public String getAddress() {
        return address;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "RequestContext{" +
            "key='" + key + '\'' +
            ", address='" + address + '\'' +
            ", transactionId=" + transactionId +
            '}';
    }
}
