package netflix.directory.server.aeron;

import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.DeleteDecoder;
import netflix.directory.server.Persister;
import rx.Observable;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Created by rroeser on 6/2/15.
 */
public class DeleteDataHandler extends PersisterDataHandler {

    protected final DeleteDecoder deleteDecoder = new DeleteDecoder();

    public DeleteDataHandler(Persister persister) {
        super(persister);
    }

    @Override
    public Observable<ResponseContext> call(DirectBuffer buffer, Integer offset, Integer length, Header header) {
        wrapMessageHeader(buffer, offset);

        deleteDecoder.wrap(
            buffer,
            offset + messageHeaderDecoder.size(),
            messageHeaderDecoder.blockLength(),
            MESSAGE_TEMPLATE_VERSION);

        String responseChannel = deleteDecoder.responseChannel();
        String unhashedKey = deleteDecoder.key();
        String hashedkey = hashFunction.hashString(unhashedKey, Charset.defaultCharset()).toString();
        UUID transactionId = toUUID(deleteDecoder.transactionId());

        return persister
            .delete(
                Observable.just(hashedkey)
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
