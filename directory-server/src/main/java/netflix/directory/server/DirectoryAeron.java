package netflix.directory.server;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.RequestMessageDecoder;
import netflix.directory.core.protocol.RequestType;
import netflix.directory.core.protocol.ResponseCode;
import netflix.directory.core.serialization.RequestMessageReader;
import netflix.directory.core.serialization.ResponseMessageWriter;
import rx.Observable;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 5/29/15.
 */
public class DirectoryAeron {
    public static final String SUBMISSION_CHANNEL = "aeron:udp?remote=localhost:43456";
    public static final int SUBMISSION_STREAM_ID = 1;

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int MESSAGE_TEMPLATE_VERSION = 0;

    public static void main(String... args) {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        final MediaDriver mediaDriver = MediaDriver.launch(ctx.dirsDeleteOnExit(true));
        final IdleStrategy idleStrategy =
            new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

        final Persister persister = new MapDBCachePersister(new NoopPersister(), 1_000_000);
        final HashFunction hashFunction = Hashing.murmur3_128();

        final Aeron.Context context = new Aeron.Context();
        final Aeron aeron = Aeron.connect(context);

        boolean running = true;

        Observable.<RequestMessageDecoder>create(subscriber -> {
            context.newConnectionHandler((channel, streamId, sessionId, joiningPosition, sourceInformation) ->
                    System.out.println("Got a connection to channel " + channel)
            );


            Subscription submissionSubscription =
                aeron.addSubscription(SUBMISSION_CHANNEL, SUBMISSION_STREAM_ID, (buffer, offset, length, header) -> {
                    MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
                    messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);

                    RequestMessageDecoder requestMessageDecoder = new RequestMessageDecoder();
                    requestMessageDecoder.wrap(buffer, offset + messageHeaderDecoder.size(), messageHeaderDecoder.blockLength(), MESSAGE_TEMPLATE_VERSION);

                    //System.out.println("Response channel = " + requestMessageDecoder.responseChannel());

                    subscriber.onNext(requestMessageDecoder);
                });

            while (running) {
                final int fragmentsRead = submissionSubscription.poll(Integer.MAX_VALUE);
                idleStrategy.idle(fragmentsRead);
            }

            subscriber.onCompleted();
        })
        .groupBy(ks -> {
            String s = ks.responseChannel();
            return s;
        })
        .flatMap(go -> {
            Publication responsePublication = aeron.addPublication(go.getKey(), 1);

            return go
                .flatMap(requestMessage -> {
                    RequestType requestType = requestMessage.type();
                    String key = requestMessage.key();
                    String value = requestMessage.value();

                    HashCode hashedKey = hashFunction.hashString(key, Charset.defaultCharset());

                    System.out.println("Receiving Request " + requestType + " for key " + hashedKey);

                    switch (requestType) {
                        case GET:
                            return persister
                                .get(Observable.just(hashedKey.toString()))
                                .doOnNext(fromCache -> {
                                    ResponseMessageWriter rmw = ResponseMessageWriter.getInstance();
                                    rmw.encode(ResponseCode.SUCCESS, fromCache);
                                    System.out.println("Get a key from the directory " + hashedKey);

                                    rmw.encode(ResponseCode.SUCCESS, key, fromCache);

                                    UnsafeBuffer buffer = new UnsafeBuffer(rmw.getBytes());

                                    while (responsePublication.offer(buffer) < 0) {

                                    }

                                });
                        case PUT:
                            return persister
                                .put(Observable.just(key), Observable.just(value))
                                .doOnNext(ack -> {
                                    ResponseMessageWriter rmw = ResponseMessageWriter.getInstance();
                                    rmw.encode(ResponseCode.SUCCESS, key, ack.toString());

                                    UnsafeBuffer buffer = new UnsafeBuffer(rmw.getBytes());

                                    while (responsePublication.offer(buffer) < 0) {

                                    }
                                });
                        default:
                            return Observable.error(new IllegalStateException("Unsupported type: " + requestType));
                    }
                });
        })
        .toBlocking()
        .last();
    }

}
