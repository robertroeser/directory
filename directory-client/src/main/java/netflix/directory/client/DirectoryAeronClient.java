package netflix.directory.client;

import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.RequestMessageEncoder;
import netflix.directory.core.protocol.RequestType;
import netflix.directory.core.protocol.ResponseMessageDecoder;
import netflix.directory.core.serialization.RequestMessageWriter;
import netflix.directory.core.serialization.ResponseMessageReader;
import rx.Observable;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 5/29/15.
 */
public class DirectoryAeronClient {

    public static final String RESPONSE_CHANNEL = "aeron:udp?remote=localhost:43450";
    public static final int SUBMISSION_STREAM_ID = 1;

    public static final String SERVER_CHANNEL = "aeron:udp?remote=localhost:43456";
    public static final int SERVER_STREAM_ID = 1;

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int MESSAGE_TEMPLATE_VERSION = 0;
    public static final int MAX_BUFFER_LENGTH = 1024;

    public static void main (String... args) {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        final MediaDriver mediaDriver = MediaDriver.launch(ctx.dirsDeleteOnExit(true));
        final IdleStrategy idleStrategy =
            new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

        final Aeron.Context context = new Aeron.Context();
        final Aeron aeron = Aeron.connect(context);


        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

        Subscription  subscription =
            aeron.addSubscription(RESPONSE_CHANNEL, SUBMISSION_STREAM_ID, (buffer, offset, length, header) -> {
                MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
                messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);

                ResponseMessageDecoder responseMessageDecoder = new ResponseMessageDecoder();
                responseMessageDecoder.wrap(buffer, offset + messageHeaderDecoder.size(), messageHeaderDecoder.blockLength(), MESSAGE_TEMPLATE_VERSION);

                System.out.println("ResponseMessage =" + responseMessageDecoder.value() + ", key= " + responseMessageDecoder.key());
            });

        Thread t = new Thread(() -> {
            while (true) {
                final int fragmentsRead = subscription.poll(Integer.MAX_VALUE);
                idleStrategy.idle(fragmentsRead);
            }
        });
        t.setDaemon(true);
        t.start();

        Publication serverPublication = aeron.addPublication(SERVER_CHANNEL,
            SERVER_STREAM_ID);

        Observable.interval(1, TimeUnit.SECONDS)
            .doOnNext(System.out::println)
            .doOnNext(i -> {
                System.out.println("Sending message a_key" + i);

                //RequestMessageWriter rmw = RequestMessageWriter.getInstance();
                //rmw.encode(RequestType.PUT, "a_key" + i, String.valueOf(System.nanoTime()), RESPONSE_CHANNEL);

                MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
                messageHeaderEncoder.wrap(unsafeBuffer, 0, MESSAGE_TEMPLATE_VERSION);

                RequestMessageEncoder requestMessageEncoder = new RequestMessageEncoder();
                requestMessageEncoder.wrap(unsafeBuffer, messageHeaderEncoder.size());

                messageHeaderEncoder
                    .blockLength(RequestMessageEncoder.BLOCK_LENGTH)
                    .templateId(RequestMessageEncoder.TEMPLATE_ID)
                    .schemaId(RequestMessageEncoder.SCHEMA_ID)
                    .version(RequestMessageEncoder.SCHEMA_VERSION);

                requestMessageEncoder.responseChannel(RESPONSE_CHANNEL);
                requestMessageEncoder.key("a_key" + i);
                requestMessageEncoder.value(String.valueOf(System.nanoTime()));
                requestMessageEncoder.type(RequestType.PUT);

                final int length = messageHeaderEncoder.size() + requestMessageEncoder.size();

                while (serverPublication.offer(unsafeBuffer, 0, length) < 0) {

                }
            })
            .toBlocking()
            .last();
    }
}

                /*
                messageHeaderEncoder.wrap(buffer, 0, MESSAGE_TEMPLATE_VERSION);
        bidEncoder.wrap(buffer, messageHeaderEncoder.size());

        messageHeaderEncoder
            .blockLength(BidEncoder.BLOCK_LENGTH)
            .templateId(BidEncoder.TEMPLATE_ID)
            .schemaId(BidEncoder.SCHEMA_ID)
            .version(BidEncoder.SCHEMA_VERSION);

        bidEncoder
            .auctionId(auctionId)
            .bidderId(bidderId)
            .value(value);
                 */