package netflix.directory.server;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import netflix.directory.core.protocol.GetDecoder;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.PutDecoder;
import netflix.directory.core.protocol.ResponseCode;
import netflix.directory.core.protocol.ResponseEncoder;
import rx.Observable;
import rx.observables.GroupedObservable;
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
    public static final int MAX_BUFFER_LENGTH = 1024;
    public static final String DIRECTORY_SERVER_CHANNEL = "aeron:udp?remote=localhost:43456";
    public static final int DIRECTORY_SERVER_STREAM_ID = 1;

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int MESSAGE_TEMPLATE_VERSION = 0;

    final MediaDriver.Context ctx = new MediaDriver.Context();
    final MediaDriver mediaDriver = MediaDriver.launch(ctx.dirsDeleteOnExit(true));
    final IdleStrategy idleStrategy =
        new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
    final Persister persister = new MapDBCachePersister(new NoopPersister(), 1_000_000);
    final HashFunction hashFunction = Hashing.murmur3_128();

    final Aeron.Context context = new Aeron.Context();
    final Aeron aeron = Aeron.connect(context);

    final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    final ResponseEncoder responseEncoder = new ResponseEncoder();

    final PutDecoder putDecoder = new PutDecoder();
    final GetDecoder getDecoder = new GetDecoder();

    final UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

    boolean running = true;

    public static void main(String... args) {
        DirectoryAeron directory = new DirectoryAeron();
        directory.start();
    }


    public void start() {
        Observable.<Observable<ResponseEncoder>>create(subscriber -> {
            context.newConnectionHandler((channel, streamId, sessionId, joiningPosition, sourceInformation) ->
                    System.out.println("Got a connection to channel " + channel)
            );

            Subscription directoryServerSubscription =
                aeron
                    .addSubscription(
                        DIRECTORY_SERVER_CHANNEL,
                        DIRECTORY_SERVER_STREAM_ID,
                        (buffer, offset, length, header) -> {
                            messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);

                            int templateId = messageHeaderDecoder.templateId();

                            System.out.println(Thread.currentThread().getName() + " Processing request with template id = " + templateId);

                            Observable<ResponseEncoder> response = null;
                            String responseChannel = null;

                            switch (templateId) {
                                case PutDecoder.TEMPLATE_ID:
                                    putDecoder.wrap(buffer,
                                        offset + messageHeaderDecoder.size(),
                                        messageHeaderDecoder.blockLength(),
                                        MESSAGE_TEMPLATE_VERSION);

                                    responseChannel = putDecoder.responseChannel();
                                    String hashedKey =
                                        hashFunction.hashString(putDecoder.key(), Charset.defaultCharset()).toString();
                                    String value = putDecoder.value();


                                    response = persister
                                        .put(Observable.just(hashedKey), Observable.just(value))
                                        .map(f -> {
                                            System.out.println("Adding key = " + hashedKey + ", value = " + value);

                                            messageHeaderEncoder.wrap(responseBuffer, 0, MESSAGE_TEMPLATE_VERSION);
                                            responseEncoder.wrap(responseBuffer, messageHeaderEncoder.size());

                                            messageHeaderEncoder
                                                .blockLength(ResponseEncoder.BLOCK_LENGTH)
                                                .templateId(ResponseEncoder.TEMPLATE_ID)
                                                .schemaId(ResponseEncoder.SCHEMA_ID)
                                                .version(ResponseEncoder.SCHEMA_VERSION);

                                            responseEncoder
                                                .code(ResponseCode.SUCCESS)
                                                .value("ok");

                                            responseEncoder.key(hashedKey.toString());

                                            return responseEncoder;
                                        });

                                    break;
                                case GetDecoder.TEMPLATE_ID:
                                    getDecoder.wrap(buffer,
                                        offset + messageHeaderDecoder.size(),
                                        messageHeaderDecoder.blockLength(),
                                        MESSAGE_TEMPLATE_VERSION);

                                    hashedKey =
                                        hashFunction.hashString(getDecoder.key(), Charset.defaultCharset()).toString();

                                    responseChannel = getDecoder.responseChannel();

                                    response = persister
                                        .get(Observable.just(getDecoder.key()))
                                        .map(f -> {
                                            messageHeaderEncoder.wrap(responseBuffer, 0, MESSAGE_TEMPLATE_VERSION);
                                            responseEncoder.wrap(responseBuffer, messageHeaderEncoder.size());

                                            messageHeaderEncoder
                                                .blockLength(ResponseEncoder.BLOCK_LENGTH)
                                                .templateId(ResponseEncoder.TEMPLATE_ID)
                                                .schemaId(ResponseEncoder.SCHEMA_ID)
                                                .version(ResponseEncoder.SCHEMA_VERSION);

                                            responseEncoder
                                                .code(ResponseCode.SUCCESS)
                                                .value(f);

                                            responseEncoder.key(hashedKey.toString());

                                            return responseEncoder;
                                        });

                                    break;
                                default:
                                    response = Observable.error(new IllegalStateException("Unsupported type: " + templateId));
                                    break;
                            }

                            Observable<ResponseEncoder> responseEncoderObservable = Observable
                                .just(GroupedObservable.from(responseChannel, response))
                                .flatMap(go -> {
                                    System.out.println(Thread.currentThread().getName() + "  Opening channel to " + go.getKey());

                                    Publication responsePublication = aeron.addPublication(go.getKey(), 1);

                                    return go
                                        .doOnNext(r -> {
                                            System.out.println(Thread.currentThread().getName() + "  Sending response to client");

                                            final int l = messageHeaderEncoder.size() + r.size();

                                            while (responsePublication.offer(responseBuffer, 0, l) < 0) {

                                            }

                                        });
                                });

                            subscriber.onNext(responseEncoderObservable);
                        });

            while (running) {
                final int fragmentsRead = directoryServerSubscription.poll(Integer.MAX_VALUE);
                idleStrategy.idle(fragmentsRead);
            }

            subscriber.onCompleted();
        })
        .flatMap(responseEncoder -> responseEncoder)
        .toBlocking()
        .last();
    }
}