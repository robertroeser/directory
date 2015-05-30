package netflix.directory.server;

import rx.Observable;

/**
 * Created by rroeser on 5/28/15.
 */
public class NoopPersister implements Persister {
    @Override
    public Observable<String> get(Observable<String> key) {
        return Observable.empty();
    }

    @Override
    public Observable<Boolean> put(Observable<String> key, Observable<String> value) {
        return Observable.just(true);
    }

    @Override
    public Observable<Boolean> delete(Observable<String> key) {
        return Observable.just(true);
    }
}
