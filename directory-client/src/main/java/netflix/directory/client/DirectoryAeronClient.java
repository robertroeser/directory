package netflix.directory.client;

import netflix.directory.core.aeron.MediaDriverHolder;
import rx.Observable;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 5/29/15.
 */
public class DirectoryAeronClient {

    public static void main (String... args) {
        final MediaDriverHolder mediaDriverHolder = MediaDriverHolder.getInstance();
        final DirectoryClient directoryClient = DirectoryClient.getInstance();

        Observable
            .interval(1, TimeUnit.SECONDS)
            .flatMap(i ->
                directoryClient
                    .put("a_key" + i, "some value " + i)
                    .doOnNext(v -> System.out.println("Putting key a_key" + i))
                    .flatMap(v ->
                        directoryClient
                            .get("a_key" + i)
                            .doOnNext(s -> System.out.println("For key a_key " + i + " got value " + s))
                    )
            )
            .toBlocking()
            .last();
    }
}
