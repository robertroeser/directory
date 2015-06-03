package netflix.directory.server.aeron;

import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.GetDecoder;
import netflix.directory.server.Persister;
import rx.Observable;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public class GetDataHandler extends PersisterDataHandler {
    protected final GetDecoder getDecoder = new GetDecoder();

    public GetDataHandler(Persister persister) {
        super(persister);
    }

    @Override
    public Observable<ResponseContext> call(DirectBuffer buffer, Integer offset, Integer length, Header header) {
        wrapMessageHeader(buffer, offset);

        getDecoder.wrap(
            buffer,
            offset + messageHeaderDecoder.size(),
            messageHeaderDecoder.blockLength(),
            MESSAGE_TEMPLATE_VERSION);

        String responseChannel = getDecoder.responseChannel();
        String unhashedKey = getDecoder.key();
        String hashedKey = hashFunction.hashString(unhashedKey, Charset.defaultCharset()).toString();
        UUID transactionId = toUUID(getDecoder.transactionId());

        return persister
            .get(
                Observable.just(hashedKey)
            )
            .map(r -> new ResponseContext(responseChannel, unhashedKey, r, ResponseContext.ResponseCode.success, transactionId))
            .onErrorResumeNext(t ->
                Observable.just(
                    new ResponseContext(responseChannel,
                        unhashedKey,
                        "error" + t.getMessage(),
                        ResponseContext.ResponseCode.error,
                        transactionId)));
    }
}
