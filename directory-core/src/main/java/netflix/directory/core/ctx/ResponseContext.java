package netflix.directory.core.ctx;

import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public class ResponseContext {
    private final String responseChannel;
    private final String unhashedKey;
    private final String response;
    private final ResponseCode responseCode;
    private final UUID transactionId;

    public enum ResponseCode { success, error, terminate }

    public ResponseContext(String responseChannel, String unhashedKey, String response, ResponseCode responseCode, UUID transactionId) {
        this.responseChannel = responseChannel;
        this.unhashedKey = unhashedKey;
        this.response = response;
        this.responseCode = responseCode;
        this.transactionId = transactionId;
    }

    public String getResponseChannel() {
        return responseChannel;
    }

    public String getUnhashedKey() {
        return unhashedKey;
    }

    public String getResponse() {
        return response;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "ResponseContext{" +
            "responseChannel='" + responseChannel + '\'' +
            ", unhashedKey='" + unhashedKey + '\'' +
            ", response='" + response + '\'' +
            ", responseCode=" + responseCode +
            ", transactionId=" + transactionId +
            '}';
    }
}
