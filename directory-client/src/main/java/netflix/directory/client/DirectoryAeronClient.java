package netflix.directory.client;

import netflix.directory.core.aeron.MediaDriverHolder;
import netflix.directory.core.util.Loggable;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 5/29/15.
 */
public class DirectoryAeronClient {
    public static void main (String... args) {
        final MediaDriverHolder mediaDriverHolder = MediaDriverHolder.getInstance();
        final DirectoryClient directoryClient = DirectoryClient.getInstance();

        long start = System.nanoTime();

        Observable
            .range(1, 10)
            .doOnNext(System.out::println)
            .flatMap(i ->
                    directoryClient
                        .put(Observable.just("a_key" + i), Observable.just("some value " + i))
                        .doOnNext(v -> Loggable.logger(DirectoryAeronClient.class).info("Putting key a_key" + i))
                        .flatMap(v ->
                                directoryClient
                                    .get(Observable.just("a_key" + i))
                                    .doOnNext(s ->
                                        Loggable.logger(DirectoryAeronClient.class).info("For key a_key" + i + " got value " + s))
                        )
            )
                    .doOnCompleted(() -> System.out.println("IM DONE"))
                    .toBlocking().lastOrDefault(null);

        long stop = System.nanoTime();

        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();

        System.out.println("IT TOOK " + (stop - start));
    }
}
