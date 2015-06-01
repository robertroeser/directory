package netflix.directory.client;

import netflix.directory.core.protocol.GetEncoder;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.PutEncoder;
import netflix.directory.core.protocol.ResponseDecoder;
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

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final PutEncoder putEncoder = new PutEncoder();
        final GetEncoder getEncoder = new GetEncoder();

        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

        Subscription  subscription =
            aeron.addSubscription(RESPONSE_CHANNEL, SUBMISSION_STREAM_ID, (buffer, offset, length, header) -> {
                MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
                messageHeaderDecoder.wrap(buffer, offset, MESSAGE_TEMPLATE_VERSION);

                ResponseDecoder responseMessageDecoder = new ResponseDecoder();
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
                System.out.println("Sending put message a_key" + i);

                messageHeaderEncoder.wrap(unsafeBuffer, 0, MESSAGE_TEMPLATE_VERSION);
                putEncoder.wrap(unsafeBuffer, messageHeaderEncoder.size());

                messageHeaderEncoder
                    .blockLength(PutEncoder.BLOCK_LENGTH)
                    .templateId(PutEncoder.TEMPLATE_ID)
                    .schemaId(PutEncoder.SCHEMA_ID)
                    .version(PutEncoder.SCHEMA_VERSION);

                putEncoder.responseChannel(RESPONSE_CHANNEL);
                putEncoder.key("a_key" + i);
                putEncoder.value(String.valueOf(System.nanoTime()));

                final int length = messageHeaderEncoder.size() + putEncoder.size();

                while (serverPublication.offer(unsafeBuffer, 0, length) < 0) {

                }
            })
            .skip(3)
            .doOnNext(i -> {
                System.out.println("Sending get message with a_key" + i);

                messageHeaderEncoder.wrap(unsafeBuffer, 0, MESSAGE_TEMPLATE_VERSION);
                getEncoder.wrap(unsafeBuffer, messageHeaderEncoder.size());

                messageHeaderEncoder
                    .blockLength(GetEncoder.BLOCK_LENGTH)
                    .templateId(GetEncoder.TEMPLATE_ID)
                    .schemaId(GetEncoder.SCHEMA_ID)
                    .version(GetEncoder.SCHEMA_VERSION);

                getEncoder.responseChannel(RESPONSE_CHANNEL);
                getEncoder.key("a_key" + i);

                final int length = messageHeaderEncoder.size() + getEncoder.size();

                while (serverPublication.offer(unsafeBuffer, 0, length) < 0) {

                }

            })
            .toBlocking()
            .last();
    }
}
