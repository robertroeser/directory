package netflix.directory.client;

import netflix.directory.core.protocol.ConnectionAckEncoder;
import netflix.directory.core.protocol.ConnectionDecoder;
import netflix.directory.core.protocol.MessageHeaderDecoder;
import netflix.directory.core.protocol.MessageHeaderEncoder;
import netflix.directory.core.protocol.PayloadDecoder;
import netflix.directory.core.protocol.PayloadResponceEncoder;
import netflix.directory.core.util.TransactionIdUtil;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/4/15.
 */
public class RAAAAR {

    private Long2ObjectHashMap<Connection> connectionsMap = new Long2ObjectHashMap<>();

    public static class Connection {
        private final Aeron aeron;

         final PublishSubject<DirectBuffer> input = PublishSubject.create();

        private final long connectionId;

        private String clientChannel;

        private static final int STREAM_ID = 1;

        private OperatorPublish operatorPublish;

        private MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        private PayloadResponceEncoder payloadResponceEncoder = new PayloadResponceEncoder();

        private Long2ObjectHashMap<Connection> connectionsMap;

        private static final ThreadLocal<Long> transcationId = new ThreadLocal<>();

        Connection(Aeron aeron, long connectionId, String clientChannel, Long2ObjectHashMap<Connection> connectionsMap) {
            this.aeron = aeron;
            this.connectionId = connectionId;
            this.clientChannel = clientChannel;
            this.connectionsMap = connectionsMap;
            this.operatorPublish = new OperatorPublish(Schedulers.computation(),
                aeron.addPublication(clientChannel, STREAM_ID));

        }

        public Observable<Void> offer(Observable<DirectBuffer> buffer) {
            return buffer
                .map(b -> {
                    UnsafeBuffer responseBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
                    messageHeaderEncoder.wrap(responseBuffer, 0, 0);
                    messageHeaderEncoder
                        .blockLength(PayloadResponceEncoder.BLOCK_LENGTH)
                        .templateId(PayloadResponceEncoder.TEMPLATE_ID)
                        .schemaId(PayloadResponceEncoder.SCHEMA_ID)
                        .version(PayloadResponceEncoder.SCHEMA_VERSION);

                    payloadResponceEncoder.wrap(responseBuffer, messageHeaderEncoder.size());
                    payloadResponceEncoder.transactionId(Connection.transcationId.get());
                    final byte[] bytes = b.byteArray();
                    payloadResponceEncoder.putBinDataEncoding(bytes, 0, bytes.length);
                    return responseBuffer;
                })
                .lift(operatorPublish)
                .doOnError(t -> {
                    if (t instanceof NotConnectedException) {
                        connectionsMap.remove(connectionId);
                    }
                })
                .map(r -> null);
        }

        // DataHandler uses this to send a message to the publish subject
        // for someone to handle
        void onNext(DirectBuffer buffer) {
            input.onNext(buffer);
        }

        Observable<Long> establishConnection() {
            return null;
        }

        void ackConnection() {
            Observable<DirectBuffer> ackBufferObservable = Observable
                .<DirectBuffer>create(subscriber -> {
                    UnsafeBuffer ackBuffer
                        = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));

                    messageHeaderEncoder.wrap(ackBuffer, 0, 0);

                    messageHeaderEncoder
                        .blockLength(ConnectionAckEncoder.BLOCK_LENGTH)
                        .templateId(ConnectionAckEncoder.TEMPLATE_ID)
                        .schemaId(ConnectionAckEncoder.SCHEMA_ID)
                        .version(ConnectionAckEncoder.SCHEMA_VERSION);

                    ConnectionAckEncoder connectionAckEncoder = new ConnectionAckEncoder();
                    connectionAckEncoder.wrap(ackBuffer, messageHeaderEncoder.size());
                    connectionAckEncoder.connectionId(connectionId);

                    subscriber.onNext(ackBuffer);
                    subscriber.onCompleted();
                })
                .lift(operatorPublish)
                .doOnError(t -> {
                    if (t instanceof NotConnectedException) {
                        connectionsMap.remove(connectionId);
                    }
                })
                .map(r -> null);

            ackBufferObservable.subscribe();
        }

         Observable<DirectBuffer> getInput() {
            return input;
        }

         long getConnectionId() {
            return connectionId;
        }

         String getClientChannel() {
            return clientChannel;
        }


         long getTransactionId() {
            return transcationId.get();
        }

         void setTranscationId(long transcationId) {
            Connection.transcationId.set(transcationId);
        }
    }

    static class OperatorPublish implements Observable.Operator<Long, DirectBuffer> {
        private Scheduler scheduler;

        private String clientChannel;

        private static final int STREAM_ID = 1;

        private Publication publication;

        public OperatorPublish(Scheduler scheduler, Publication publication) {
            this.scheduler = scheduler;
            this.publication = publication;
        }

        @Override
        public Subscriber<? super DirectBuffer> call(Subscriber<? super Long> child) {
            return new Subscriber<DirectBuffer>(child) {

                Scheduler.Worker worker = scheduler.createWorker();

                @Override
                public void onStart() {
                    request(1);
                    child.add(worker);
                    worker = scheduler.createWorker();
                }

                @Override
                public void onCompleted() {
                    child.onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    child.onError(e);
                }

                @Override
                public void onNext(DirectBuffer directBuffer) {
                    tryOffer(directBuffer);
                }

                public void tryOffer(DirectBuffer buffer) {
                    long offer = publication.offer(buffer);

                    if (offer == 0) {
                        child.onNext(offer);
                        request(1);
                    } else if (offer == Publication.NOT_CONNECTED) {
                        child.onError(new NotConnectedException());
                    } else if (offer == Publication.BACK_PRESSURE) {
                        worker.schedule(() ->
                                tryOffer(buffer)
                        );
                    }

                }

            };
        }
    }

    public static class NotConnectedException extends Exception {

    }

    protected final Aeron.Context context;
    protected final Aeron aeron;

    private RAAAAR() {
        this.context = new Aeron.Context();
        this.aeron = Aeron.connect(context);
    }

    public Observable<Void> createServer(String channel, Func1<Connection, Observable<Void>> handler) {
        return Observable.<Void>create(subscribe -> {
            Subscription subscription = aeron.addSubscription(channel, 0, new FragmentAssemblyAdapter(new DataHandler() {
                MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
                ConnectionDecoder connectionDecoder = new ConnectionDecoder();
                PayloadDecoder payloadDecoder = new PayloadDecoder();

                @Override
                public void onData(DirectBuffer buffer, int offset, int length, Header header) {
                    messageHeaderDecoder.wrap(buffer, offset, 0);

                    int templateId = messageHeaderDecoder.templateId();

                    switch (templateId) {
                        case ConnectionDecoder.TEMPLATE_ID:
                            connectionDecoder.wrap(buffer,
                                offset + messageHeaderDecoder.size(),
                                messageHeaderDecoder.blockLength(),
                                0);

                            String clientChannel = connectionDecoder.clientChannel();

                            long connectionId = TransactionIdUtil.getConnectionId();
                            Connection connection = new Connection(aeron, connectionId, clientChannel, connectionsMap);

                            connectionsMap.put(connectionId, connection);

                            connection.ackConnection();

                            break;
                        case PayloadDecoder.TEMPLATE_ID:
                            payloadDecoder.wrap(buffer,
                                offset + messageHeaderDecoder.size(),
                                messageHeaderDecoder.blockLength(),
                                0);

                            connectionId = payloadDecoder.connectionId();

                            byte[] bytes = new byte[payloadDecoder.binDataEncodingLength()];
                            payloadDecoder.getBinDataEncoding(bytes, 0, bytes.length);

                            connection = connectionsMap.get(connectionId);
                            connection.onNext(new UnsafeBuffer(bytes));

                            handler
                                .call(connection)
                                .subscribeOn(Schedulers.computation())
                                .subscribe();

                            break;
                        default:

                    }
                }
            }));

            Scheduler.Worker worker = Schedulers.computation().createWorker();
            subscribe.add(worker);
            worker.schedulePeriodically(() -> subscription.poll(100), 0, 0, TimeUnit.NANOSECONDS);
            subscribe.onNext(null);
        });

    }

    public class Client<T,R> implements Func1<Observable<T>, Observable<R>> {
        private Connection connection;

        private Func1<T, byte[]> encode;
        private Func1<byte[], R> decode;

        public Client(Connection connection, Func1<T, byte[]> encode, Func1<byte[], R> decode) {
            this.connection = connection;
            this.encode = encode;
            this.decode = decode;
        }

        @Override
        public Observable<R> call(Observable<T> tObservable) {
            return null;
        }
    }

    public <T,R> Observable<Client<T,R>> createClient(String channel, Func1<T, byte[]> encode, Func1<byte[], R> decode) {

         return Observable.<Observable<Connection>>create(subscriber -> {
             Connection connection = new Connection(aeron, -1, channel, connectionsMap);
             subscriber.onNext(Observable.just(connection));
         })
        .flatMap(oc -> {
            return oc
                .flatMap(c -> {
                    return c.establishConnection().doOnNext(l -> c.setTranscationId(l));
                })
                .retry(10)
                .flatMap(f -> oc);

        })
        .map(connection ->
            new Client<>(connection, encode, decode));
    }


}
