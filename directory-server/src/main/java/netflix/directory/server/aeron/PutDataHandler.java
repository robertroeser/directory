package netflix.directory.server.aeron;

import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.PutDecoder;
import netflix.directory.server.Persister;
import rx.Observable;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public class PutDataHandler extends PersisterDataHandler {

    protected final PutDecoder putDecoder = new PutDecoder();

    public PutDataHandler(Persister persister) {
        super(persister);
    }

    @Override
    public Observable<ResponseContext> call(DirectBuffer buffer, Integer offset, Integer length, Header header) {
        wrapMessageHeader(buffer, offset);

        putDecoder.wrap(
            buffer,
            offset + messageHeaderDecoder.size(),
            messageHeaderDecoder.blockLength(),
            MESSAGE_TEMPLATE_VERSION);

        String responseChannel = putDecoder.responseChannel();
        String unhashedKey = putDecoder.key();
        String hashedkey = hashFunction.hashString(unhashedKey, Charset.defaultCharset()).toString();
        String value = putDecoder.value();
        UUID transactionId = toUUID(putDecoder.transactionId());

        return persister
            .put(
                Observable.just(hashedkey),
                Observable.just(value)
            )
            .map(r -> new ResponseContext(responseChannel, unhashedKey, String.valueOf(r), ResponseContext.ResponseCode.success, transactionId))
            .onErrorResumeNext(t ->
                Observable.just(
                    new ResponseContext(responseChannel,
                        unhashedKey,
                        "error" + t.getMessage(),
                        ResponseContext.ResponseCode.error,
                        transactionId)));

    }
}
