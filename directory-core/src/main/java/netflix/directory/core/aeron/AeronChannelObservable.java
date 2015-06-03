package netflix.directory.core.aeron;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.exceptions.MissingBackpressureException;
import rx.functions.Func4;
import rx.internal.util.RxRingBuffer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/1/15.
 */
public class AeronChannelObservable implements AutoCloseable {

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);

    protected final Aeron.Context context;
    protected final Aeron aeron;

    protected final IdleStrategy idleStrategy =
        new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

    private volatile boolean running;

    protected AeronChannelObservable() {
        this.context = new Aeron.Context();
        this.aeron = Aeron.connect(context);
        this.running = true;
    }

    public static AeronChannelObservable create() {
        return new AeronChannelObservable();
    }

    @Override
    public void close() throws Exception {
        try {
            aeron.close();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            running = false;
        }
    }

    public interface ConsumeDataHandler<R> extends Func4<DirectBuffer, Integer, Integer, Header, R> {
        @Override
        R call(DirectBuffer buffer, Integer offset, Integer length, Header header);
    }

    public <T> Observable<T> consume(final String channel,
                                     final int streamId,
                                     final ConsumeDataHandler<T> handler,
                                     final Scheduler scheduler) {

        return Observable.create(subscriber -> {
            uk.co.real_logic.aeron.Subscription subscription =
                aeron.addSubscription(channel, streamId,
                    new FragmentAssemblyAdapter(
                    (buffer, offset, length, header) -> {
                        T consumable = handler.call(buffer, offset, length, header);
                        subscriber.onNext(consumable);
                }));

            subscriber.add(new Subscription() {
                @Override
                public void unsubscribe() {
                    subscription.close();
                }

                @Override
                public boolean isUnsubscribed() {
                    return subscriber.isUnsubscribed();
                }
            });

            Scheduler.Worker worker = scheduler.createWorker();
            subscriber.add(worker);
            worker.schedulePeriodically(
                () -> {
                    if (running) {
                        int fragmentsRead = subscription.poll(Integer.MAX_VALUE);
                        idleStrategy.idle(fragmentsRead);
                    } else {
                        subscriber.onCompleted();
                    }
                }, 0, 1, TimeUnit.MILLISECONDS
            );

        });

    }

    public Observable<Void> publish(final String channel,
                                    final int streamId,
                                    final Observable<DirectBuffer> data,
                                    final Scheduler scheduler) {
        return data
            .lift(new Observable.Operator<Void, DirectBuffer>() {
                Publication publication = aeron.addPublication(channel, streamId);

                RxRingBuffer buffer = RxRingBuffer.getSpmcInstance();

                @Override
                public Subscriber<? super DirectBuffer> call(Subscriber<? super Void> child) {
                    return new Subscriber<DirectBuffer>(child) {
                        @Override
                        public void onStart() {
                            add(new Subscription() {
                                @Override
                                public void unsubscribe() {
                                    publication.close();
                                }

                                @Override
                                public boolean isUnsubscribed() {
                                    return child.isUnsubscribed();
                                }
                            });
                            request(buffer.available());
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
                            long offer = publication.offer(directBuffer);

                            checkOffer(directBuffer, offer);
                        }

                        public void checkOffer(DirectBuffer directBuffer, long offer) {
                            if (offer == Publication.NOT_CONNECTED) {
                                onError(new IllegalStateException("not connected"));
                            } else if (offer == Publication.BACK_PRESSURE) {
                                while (buffer.available() <= 0) {
                                }

                                try {
                                    buffer.onNext(directBuffer);
                                } catch (MissingBackpressureException e) {
                                    onError(e);
                                }

                                Scheduler.Worker worker = scheduler.createWorker();
                                child.add(worker);
                                worker.schedule(() -> {
                                    DirectBuffer fromRingBuffer = null;
                                    while ((fromRingBuffer = (DirectBuffer) buffer.poll()) != null) {
                                        checkOffer(fromRingBuffer, publication.offer(fromRingBuffer));
                                    }
                                });
                            } else {
                                request(buffer.available());
                            }
                        }
                    };
                }
            });
    }
}
