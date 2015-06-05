package netflix.directory.client;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.util.concurrent.ListenableFuture;
import netflix.directory.core.aeron.AeronChannelObservable;
import netflix.directory.core.ctx.RequestContext;
import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.GetEncoder;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.PutEncoder;
import netflix.directory.core.protocol.ResponseDecoder;
import netflix.directory.core.util.Loggable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observables.ConnectableObservable;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import sun.plugin2.jvm.RemoteJVMLauncher;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import javax.security.auth.callback.Callback;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/2/15.
 */
public class DirectoryClient implements Loggable {

    public static final int MAX_BUFFER_LENGTH = 1024;

    public static final String DIRECTORY_SERVER_CHANNEL = "aeron:udp?remote=localhost:43456";
    public static final String DIRECTORY_CLIENT_CHANNEL = "aeron:udp?remote=localhost:50000";

    public static final int PUT_STREAM_ID = 1;
    public static final int GET_STREAM_ID = 2;
    public static final int DELETE_STREAM_ID = 3;

    private static DirectoryClient INSTANCE;

    private final UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final PutEncoder putEncoder = new PutEncoder();
    private final GetEncoder getEncoder = new GetEncoder();
    private final ResponseDecoder responseDecoder = new ResponseDecoder();

    private final AsyncSubject<RequestContext> putPublicationSubject;
    private final AsyncSubject<RequestContext> getPublicationSubject;

    private final ConcurrentSkipListMap<UUID, Subscriber<? super ResponseContext>> subscriberMap;

    private final ConcurrentSkipListMap<UUID, AsyncSubject<? super ResponseContext>> subjectMap;


    private final ConcurrentSkipListMap<UUID, Callback> callbackMap;

    private final TimeBasedGenerator timeBasedGenerator;

    private AtomicLong lastRunCleanupTs = new AtomicLong();

    private final AeronChannelObservable aeronChannelObservable;

    private DirectoryClient() {
        subscriberMap = new ConcurrentSkipListMap<>((o1, o2) ->  o2.compareTo(o1));
        subjectMap = new ConcurrentSkipListMap<>((o1, o2) ->  o2.compareTo(o1));
        callbackMap = new ConcurrentSkipListMap<>((o1, o2) ->  o2.compareTo(o1));


        timeBasedGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface());

        aeronChannelObservable = AeronChannelObservable.create();

        putPublicationSubject = AsyncSubject.create();
        getPublicationSubject = AsyncSubject.create();

        Observable<Void> putPublish = aeronChannelObservable
            .publish(
                DIRECTORY_SERVER_CHANNEL,
                PUT_STREAM_ID,
                putPublicationSubject
                    .map(this::toPutRequest),
                Schedulers.computation());

        Observable<Void> putConsume = doConsume(PUT_STREAM_ID);

        Observable<Void> getPublish = aeronChannelObservable
            .publish(
                DIRECTORY_SERVER_CHANNEL,
                GET_STREAM_ID,
                getPublicationSubject
                    .map(this::toGetRequest),
                Schedulers.computation());

        Observable<Void> getConsume = doConsume(GET_STREAM_ID);

        Scheduler.Worker worker = Schedulers.newThread().createWorker();
        worker.schedulePeriodically(() -> {
            List<UUID> timedOut = new ArrayList<>();

            subscriberMap.forEach((uuid, ctx) -> {
                long currentTs = System.currentTimeMillis();
                lastRunCleanupTs.set(currentTs);

                if (currentTs - uuid.timestamp() > TimeUnit.SECONDS.toMillis(2)) {
                    timedOut.add(uuid);
                }
            });

            timedOut.forEach(subscriberMap::remove);

        }, 0, 100, TimeUnit.MILLISECONDS);

        Observable
            .merge(putPublish, putConsume, getPublish, getConsume)
            .subscribeOn(Schedulers.newThread())
            .subscribe();
    }

    public static DirectoryClient getInstance() {
        if (INSTANCE == null) {
            init();
        }

        return INSTANCE;
    }

    private static synchronized void init() {
        if (INSTANCE == null) {
            INSTANCE = new DirectoryClient();
        }
    }

    public Observable<Void> put(Observable<String> key, Observable<String> value) {
        AsyncSubject<? super ResponseContext> asyncSubject = AsyncSubject.create();

        Observable<DirectBuffer> buffer = key
            .zipWith(value, (k, v) -> {

                final UUID transactionId = generateTransactionId();
                subjectMap.put(transactionId, asyncSubject);

                DirectBuffer directBuffer = toPutRequest(new RequestContext(k, v, transactionId));
                logger().debug("Putting with transaction id {} ", transactionId);

                return directBuffer;
            });

        aeronChannelObservable
            .publish(DIRECTORY_SERVER_CHANNEL, PUT_STREAM_ID, buffer, Schedulers.computation()).subscribe();

        return asyncSubject
                    .map(r -> null);
    }

    public Observable<String> get(Observable<String> key) {
        AsyncSubject<? super ResponseContext> asyncSubject = AsyncSubject.create();

        Observable<DirectBuffer> buffer = key
            .map(k -> {
                final UUID transactionId = generateTransactionId();
                subjectMap.put(transactionId, asyncSubject);


                DirectBuffer directBuffer = toGetRequest(new RequestContext(k, "", transactionId));
                logger().debug("Getting with transaction id {} ", transactionId);
                return directBuffer;
            });

         aeronChannelObservable
            .publish(DIRECTORY_SERVER_CHANNEL, GET_STREAM_ID, buffer, Schedulers.computation()).subscribe();


        return asyncSubject.map(r -> null);

        /*return Observable
            .<ResponseContext>create(subscriber -> {
                final UUID transactionId = generateTransactionId();

                logger().debug("Getting with transaction id {}", transactionId);

                subscriberMap.put(transactionId, subscriber);
                getPublicationSubject.onNext(new RequestContext(key, "", transactionId));
            })
            .doOnNext(r -> logger().debug("Get response = {}  with transaction id {}", r.toString(), r.getTransactionId()))
            .map(r -> r.getResponse());*/
    }

    protected Observable<Void> doConsume(int streamId) {
        return aeronChannelObservable
            .consume(DIRECTORY_CLIENT_CHANNEL, streamId,
                new AeronChannelObservable.ConsumeDataHandler<ResponseContext>() {
                    protected static final int MESSAGE_TEMPLATE_VERSION = 0;

                    @Override
                    public ResponseContext call(DirectBuffer buffer, Integer offset, Integer length, Header header) {
                        messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);

                        responseDecoder.wrap(
                            buffer,
                            offset + messageHeaderDecoder.size(),
                            messageHeaderDecoder.blockLength(),
                            MESSAGE_TEMPLATE_VERSION);

                        String key = responseDecoder.key();
                        String value = responseDecoder.value();

                        ResponseContext.ResponseCode responseCode = null;

                        switch (responseDecoder.code()) {
                            case SUCCESS:
                                responseCode = ResponseContext.ResponseCode.success;
                                break;
                            case ERROR:
                                responseCode = ResponseContext.ResponseCode.error;
                        }

                        long mostSignificationBits = responseDecoder.transactionId().mostSignificationBits();
                        long leastSignificationBits = responseDecoder.transactionId().leastSignificationBits();

                        return new ResponseContext(DIRECTORY_CLIENT_CHANNEL, key, value, responseCode, new UUID(mostSignificationBits, leastSignificationBits));
                    }
                }
                , Schedulers.computation())
            .doOnNext(r -> logger().debug("Consuming a response from stream {} = {}", streamId, r.toString()))
            .doOnNext(r -> {
                AsyncSubject<? super ResponseContext> asyncSubject = subjectMap.get(r.getTransactionId());
                if (asyncSubject != null) {
                    if (r.getResponseCode() == ResponseContext.ResponseCode.success) {
                        asyncSubject.onNext(r);
                        asyncSubject.onCompleted();
                    } else if (r.getResponseCode() == ResponseContext.ResponseCode.error) {
                        asyncSubject.onError(new RuntimeException(r.getResponse()));
                    } else {
                        asyncSubject.onCompleted();
                    }

                    subscriberMap.remove(r.getTransactionId());
                } else {
                    logger().debug("No subscriber found for transaction id {}", r.getTransactionId());
                }
/*

                Subscriber<? super ResponseContext> subscriber = subscriberMap.get(r.getTransactionId());
                if (subscriber != null) {
                    if (r.getResponseCode() == ResponseContext.ResponseCode.success) {
                        subscriber.onNext(r);
                        subscriber.onCompleted();
                    } else if (r.getResponseCode() == ResponseContext.ResponseCode.error) {
                        subscriber.onError(new RuntimeException(r.getResponse()));
                    } else {
                        subscriber.onCompleted();
                    }

                    subscriberMap.remove(r.getTransactionId());
                } else {
                    logger().debug("No subscriber found for transaction id {}", r.getTransactionId());
                }
                 */
            })
            .map(r -> null);
    }

    protected UUID generateTransactionId() {
        return timeBasedGenerator.generate();
    }

    protected DirectBuffer toPutRequest(RequestContext context) {

        messageHeaderEncoder.wrap(requestBuffer, 0, 0);

        messageHeaderEncoder
            .blockLength(PutEncoder.BLOCK_LENGTH)
            .templateId(PutEncoder.TEMPLATE_ID)
            .schemaId(PutEncoder.SCHEMA_ID)
            .version(PutEncoder.SCHEMA_VERSION);


        putEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

        putEncoder.responseChannel(DIRECTORY_CLIENT_CHANNEL);
        putEncoder
            .key(context.getKey());
        putEncoder
            .value(context.getAddress());
        putEncoder
            .transactionId().mostSignificationBits(context.getTransactionId().getMostSignificantBits());
        putEncoder
            .transactionId().leastSignificationBits(context.getTransactionId().getLeastSignificantBits());


        return requestBuffer;
    }

    protected DirectBuffer toGetRequest(RequestContext context) {

        messageHeaderEncoder.wrap(requestBuffer, 0, 0);

        messageHeaderEncoder
            .blockLength(GetEncoder.BLOCK_LENGTH)
            .templateId(GetEncoder.TEMPLATE_ID)
            .schemaId(GetEncoder.SCHEMA_ID)
            .version(GetEncoder.SCHEMA_VERSION);

        getEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

        getEncoder.responseChannel(DIRECTORY_CLIENT_CHANNEL);
        getEncoder
            .key(context.getKey());
        getEncoder
            .transactionId().mostSignificationBits(context.getTransactionId().getMostSignificantBits());
        getEncoder
            .transactionId().leastSignificationBits(context.getTransactionId().getLeastSignificantBits());


        return requestBuffer;
    }

}
