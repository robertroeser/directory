package netflix.directory.server;

import rx.Observable;

/**
 * Created by rroeser on 5/28/15.
 */
public interface Persister {
    Observable<String> get(Observable<String> key);
    Observable<Boolean> put(Observable<String> key, Observable<String> value);
    Observable<Boolean> delete(Observable<String> key);
}
