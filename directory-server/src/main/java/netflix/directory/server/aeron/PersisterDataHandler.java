package netflix.directory.server.aeron;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import netflix.directory.core.aeron.AeronChannelObservable;
import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.util.UuidUtils;
import netflix.directory.server.Persister;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;

/**
 * Created by rroeser on 6/2/15.
 */
public abstract class PersisterDataHandler  implements AeronChannelObservable.ConsumeDataHandler<Observable<ResponseContext>>, UuidUtils {

    protected static final int MESSAGE_TEMPLATE_VERSION = 0;

    protected final Persister persister;
    protected final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    protected final HashFunction hashFunction = Hashing.murmur3_128();

    public PersisterDataHandler(Persister persister) {
        this.persister = persister;
    }

    protected MessageHeaderDecoder wrapMessageHeader(DirectBuffer buffer, Integer offset) {
        return messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);
    }
}
