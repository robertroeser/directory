package netflix.directory.client;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import netflix.directory.core.aeron.AeronChannelObservable;
import netflix.directory.core.ctx.RequestContext;
import netflix.directory.core.ctx.ResponseContext;
import netflix.directory.core.protocol.GetEncoder;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.PutEncoder;
import netflix.directory.core.protocol.ResponseDecoder;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by rroeser on 6/2/15.
 */
public class DirectoryClient {

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

    private final BehaviorSubject<RequestContext> putPublicationSubject;
    private final BehaviorSubject<RequestContext> getPublicationSubject;

    private final ConcurrentSkipListMap<UUID, Subscriber<? super ResponseContext>> subscriberMap;

    private final TimeBasedGenerator timeBasedGenerator;

    private final AeronChannelObservable aeronChannelObservable;

    private DirectoryClient() {
        subscriberMap = new ConcurrentSkipListMap<>();

        timeBasedGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface());

        aeronChannelObservable = AeronChannelObservable.create();

        putPublicationSubject = BehaviorSubject.create();
        getPublicationSubject = BehaviorSubject.create();

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
                putPublicationSubject
                    .map(this::toGetRequest),
                Schedulers.computation());

        Observable<Void> getConsume = doConsume(GET_STREAM_ID);

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

    public Observable<Void> put(String key, String value) {
        return Observable
            .<ResponseContext>create(subscriber -> {
                final UUID transactionId = generateTransactionId();

                subscriberMap.put(transactionId, subscriber);
                putPublicationSubject.onNext(new RequestContext(key, value, transactionId));
            })
            .doOnNext(r -> System.out.println("Put response = " + r.toString()))
            .map(r -> null);
    }

    public Observable<String> get(String key) {
        return Observable
            .<ResponseContext>create(subscriber -> {
                final UUID transactionId = generateTransactionId();

                subscriberMap.put(transactionId, subscriber);
                getPublicationSubject.onNext(new RequestContext(key, null, transactionId));
            })
            .doOnNext(r -> System.out.println("Get response = " + r.toString()))
            .map(r -> r.getResponse());
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
            .doOnNext(r ->  System.out.println("Consuming a response from stream " + streamId + " = "  + r.toString()))
            .doOnNext(r -> {
                Subscriber<? super ResponseContext> subscriber = subscriberMap.remove(r.getTransactionId());

                if (subscriber != null) {
                    if (r.getResponseCode() == ResponseContext.ResponseCode.success) {
                        subscriber.onNext(r);
                        subscriber.onCompleted();
                    } else if (r.getResponseCode() == ResponseContext.ResponseCode.error) {
                        subscriber.onError(new RuntimeException(r.getResponse()));
                    } else {
                        subscriber.onCompleted();
                    }
                } else {
                    System.out.println("No subscriber found for transaction id " + r.getTransactionId());
                }
            })
            .map(r -> null);
    }

    protected UUID generateTransactionId() {
        return timeBasedGenerator.generate();
    }

    protected DirectBuffer toPutRequest(RequestContext context) {
        messageHeaderEncoder
            .blockLength(PutEncoder.BLOCK_LENGTH)
            .templateId(PutEncoder.TEMPLATE_ID)
            .schemaId(PutEncoder.SCHEMA_ID)
            .version(PutEncoder.SCHEMA_VERSION);

        putEncoder.responseChannel(DIRECTORY_CLIENT_CHANNEL);
        putEncoder
            .key(context.getKey());
        putEncoder
            .value(context.getAddress());
        putEncoder
            .transactionId().mostSignificationBits(context.getTransactionId().getMostSignificantBits());
        putEncoder
            .transactionId().leastSignificationBits(context.getTransactionId().getLeastSignificantBits());

        messageHeaderEncoder.wrap(requestBuffer, 0, 0);
        putEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

        return requestBuffer;
    }

    protected DirectBuffer toGetRequest(RequestContext context) {
        messageHeaderEncoder
            .blockLength(GetEncoder.BLOCK_LENGTH)
            .templateId(GetEncoder.TEMPLATE_ID)
            .schemaId(GetEncoder.SCHEMA_ID)
            .version(GetEncoder.SCHEMA_VERSION);

        getEncoder.responseChannel(DIRECTORY_CLIENT_CHANNEL);
        getEncoder
            .key(context.getKey());
        getEncoder
            .transactionId().mostSignificationBits(context.getTransactionId().getMostSignificantBits());
        getEncoder
            .transactionId().leastSignificationBits(context.getTransactionId().getLeastSignificantBits());

        messageHeaderEncoder.wrap(requestBuffer, 0, 0);
        getEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

        return requestBuffer;
    }

}
