package netflix.directory.server;

import netflix.directory.core.aeron.AeronChannelObservable;
import netflix.directory.core.aeron.MediaDriverHolder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.ResponseCode;
import netflix.directory.core.protocol.ResponseEncoder;
import netflix.directory.core.util.UuidUtils;
import netflix.directory.server.aeron.DeleteDataHandler;
import netflix.directory.server.aeron.GetDataHandler;
import netflix.directory.server.aeron.PutDataHandler;
import netflix.directory.core.ctx.ResponseContext;
import rx.Observable;
import rx.schedulers.Schedulers;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 6/2/15.
 */
public class DirectoryServer implements UuidUtils {
    public static final int MAX_BUFFER_LENGTH = 1024;

    public static final String DIRECTORY_SERVER_CHANNEL = "aeron:udp?remote=localhost:43456";
    public static final int PUT_STREAM_ID = 1;
    public static final int GET_STREAM_ID = 2;
    public static final int DELETE_STREAM_ID = 3;

    private final AeronChannelObservable aeronChannelObservable;
    private final MediaDriverHolder mediaDriverHolder;
    private final Persister persister;

    private ResponseEncoder responseEncoder = new ResponseEncoder();
    private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    private final UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

    public DirectoryServer() {
        this.mediaDriverHolder = MediaDriverHolder.getInstance();
        this.aeronChannelObservable = AeronChannelObservable.create();
        this.persister = new MapDBCachePersister(new NoopPersister(), 1_000_000);
    }

    public static void main(String... args) {
        DirectoryServer ds = new DirectoryServer();
        ds.start();
    }

    public void start() {
        Observable<Void> putObservable =
            aeronChannelObservable
                .consume(
                    DIRECTORY_SERVER_CHANNEL,
                    PUT_STREAM_ID,
                    new PutDataHandler(persister),
                    Schedulers.computation())
                .flatMap(responseContextObservable -> publishToGroup(responseContextObservable, PUT_STREAM_ID));

        Observable<Void> getObservable =
            aeronChannelObservable
                .consume(DIRECTORY_SERVER_CHANNEL,
                    GET_STREAM_ID,
                    new GetDataHandler(persister),
                    Schedulers.computation())
                .flatMap(responseContextObservable -> publishToGroup(responseContextObservable, GET_STREAM_ID));

        Observable<Void> deleteObservable =
            aeronChannelObservable
                .consume(DIRECTORY_SERVER_CHANNEL,
                    DELETE_STREAM_ID,
                    new DeleteDataHandler(persister),
                    Schedulers.computation())
                .flatMap(responseContextObservable -> publishToGroup(responseContextObservable, DELETE_STREAM_ID));

        Observable.merge(putObservable, getObservable, deleteObservable).toBlocking().last();
    }

    public Observable<Void> publishToGroup(Observable<ResponseContext> responseContextObservable, int streamId) {
        return responseContextObservable
            .groupBy(keySelector -> keySelector.getResponseChannel())
            .flatMap(go ->
                aeronChannelObservable.publish(go.getKey(), streamId,
                    go
                        .doOnSubscribe(() ->
                            System.out.println("groupby has been subscribed for "
                                + go.getKey()
                                + ", streamId "
                                + streamId))
                        .doOnUnsubscribe(() ->
                            System.out.println("groupby has been unsubscribed for "
                                + go.getKey()
                                + ", streamId "
                                + streamId))
                        .map(this::map),
                    Schedulers.computation())
                    .doOnError(Throwable::printStackTrace)
                    .onErrorResumeNext(r -> Observable.empty())
            );
    }

    public DirectBuffer map(ResponseContext context) {
        messageHeaderEncoder
            .blockLength(ResponseEncoder.BLOCK_LENGTH)
            .templateId(ResponseEncoder.TEMPLATE_ID)
            .schemaId(ResponseEncoder.SCHEMA_ID)
            .version(ResponseEncoder.SCHEMA_VERSION);

        responseEncoder
            .key(context.getUnhashedKey());
        responseEncoder
            .value(context.getResponse());

        switch (context.getResponseCode()) {
            case success:
                responseEncoder.code(ResponseCode.SUCCESS);
                break;
            case error:
                responseEncoder.code(ResponseCode.ERROR);
                break;
        }

        responseEncoder.transactionId().mostSignificationBits(context.getTransactionId().getMostSignificantBits());
        responseEncoder.transactionId().leastSignificationBits(context.getTransactionId().getLeastSignificantBits());

        messageHeaderEncoder.wrap(responseBuffer, 0, 0);
        responseEncoder.wrap(responseBuffer, messageHeaderEncoder.size());

        return responseBuffer;

    }
 }
