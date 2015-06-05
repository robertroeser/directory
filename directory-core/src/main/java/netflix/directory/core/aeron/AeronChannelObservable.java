package netflix.directory.core.aeron;

import netflix.directory.core.util.Loggable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func4;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/1/15.
 */
public class AeronChannelObservable implements AutoCloseable, Loggable {

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);

    protected final Aeron.Context context;
    protected final Aeron aeron;

    protected final IdleStrategy idleStrategy =
        new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

    private volatile boolean running;

    private ConcurrentHashMap<String, Publication> publications;

    protected AeronChannelObservable() {
        this.context = new Aeron.Context();
        this.aeron = Aeron.connect(context);
        this.running = true;
        this.publications = new ConcurrentHashMap<>();
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


        Observable<T> consumeObservable = Observable.create(subscriber -> {
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
                        subscription.poll(100);
                    } else {
                        subscriber.onCompleted();
                    }
                }, 0, 0, TimeUnit.NANOSECONDS
            );

        });

        consumeObservable = consumeObservable
            .doOnSubscribe(() -> logger().trace("doOnSubscribe consume observable for  channel {}, stream id {}", channel, streamId))
            .doOnUnsubscribe(() -> logger().trace("doOnUnsubscribe consume observable for  channel {}, stream id {}", channel, streamId));

        return consumeObservable;
    }

    protected Publication getPublication(final String channel, final int streamId) {
        return publications.computeIfAbsent(channel + streamId, (key) ->  aeron.addPublication(channel, streamId));
    }

    /*
     public static <T> Observable<Long> publish(Aeron aeron, final String channel, final int streamId, Observable<T> data, Func1<T, DirectBuffer> map) {

    return data.lift(new Operator<Long, T>() {

        @Override
        public Subscriber<? super T> call(Subscriber<? super Long> child) {
            return new Subscriber<T>(child) {

                private DirectBuffer last;
                Publication serverPublication;
                Worker w;

                @Override
                public void onStart() {
                    serverPublication = aeron.addPublication(channel, streamId);
                    add(Subscriptions.create(() -> serverPublication.close()));
                    w = Schedulers.computation().createWorker();
                    add(w);
                    // TODO make this do more than 1
                    request(1);
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
                public void onNext(T t) {
                    DirectBuffer v = map.call(t);
                    tryOffer(v);
                }

                private void tryOffer(DirectBuffer v) {
                    long sent = serverPublication.offer(v);
                    if (sent == Publication.NOT_CONNECTED) {
                        onError(new RuntimeException("Not connected"));
                    } else if (sent == Publication.BACK_PRESSURE) {
                        last = v;
                        w.schedule(() -> tryOffer(v));
                    } else {
                        child.onNext(sent);
                        // TODO make this do more than 1
                        request(1);
                    }
                }

            };
        }

    });
}

    public static <T> Observable<T> consume(Aeron aeron, final String channel, final int streamId, Func3<DirectBuffer, Integer, Integer, T> map) {
        return Observable.create(s -> {
            Subscription subscription = aeron.addSubscription(channel, streamId, new FragmentAssemblyAdapter((buffer, offset, length, header) -> {
                T value = map.call(buffer, offset, length);
                try {
                    // make it behave slowly
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                s.onNext(value);
            }));
            s.add(Subscriptions.create(() -> subscription.close()));

            // use existing event loops
            Worker w = Schedulers.computation().createWorker();
            // limit fragments so it doesn't starve the eventloop
            w.schedulePeriodically(() -> subscription.poll(100), 0, 0, TimeUnit.NANOSECONDS);
            s.add(w);
        });
    }

*/

    public Observable<Void> publish(final String channel,
                                    final int streamId,
                                    final Observable<DirectBuffer> data,
                                    final Scheduler scheduler) {
        return data
            .doOnSubscribe(() -> logger().trace("doOnSubscribe publish observable for  channel {}, stream id {}", channel, streamId))
            .doOnUnsubscribe(() -> logger().trace("doOnUnsubscribe publish observable for  channel {}, stream id {}", channel, streamId))
            .lift(new Observable.Operator<Void, DirectBuffer>() {
                @Override
                public Subscriber<? super DirectBuffer> call(Subscriber<? super Void> child) {
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
                            long offer = getPublication(channel, streamId).offer(buffer);

                            if (offer == 0) {
                                request(1);
                            } else if (offer == Publication.NOT_CONNECTED) {
                                publications.remove(channel + streamId);
                                onError(new IllegalStateException("not connected"));
                            } else if (offer == Publication.BACK_PRESSURE) {
                                worker.schedule(() ->
                                        tryOffer(buffer)
                                );
                            }
                        }

                    };
                }
            });
    }
}
