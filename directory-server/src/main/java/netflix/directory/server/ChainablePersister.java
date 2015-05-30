package netflix.directory.server;

import rx.Observable;
import rx.schedulers.Schedulers;

/**
 * Created by rroeser on 5/28/15.
 */
public abstract class ChainablePersister implements Persister {
    private Persister child;

    public ChainablePersister(Persister child) {
        this.child = child;
    }

    @Override
    public Observable<String> get(Observable<String> key) {
        return doGet(key)
            .switchIfEmpty(child.get(key));
    }

    @Override
    public Observable<Boolean> put(Observable<String> key, Observable<String> value) {
        return doPut(key, value)
            .filter(result ->
                result == true
            )
            .flatMap(f ->
                child.put(key, value).subscribeOn(Schedulers.computation())
            );
    }

    @Override
    public Observable<Boolean> delete(Observable<String> key) {
        return doDelete(key)
            .filter(result ->
                result == true
            )
            .flatMap(f ->
                child.delete(key).subscribeOn(Schedulers.computation())
            );
    }

    protected abstract Observable<String> doGet(Observable<String> key);

    protected abstract Observable<Boolean> doPut(Observable<String> key, Observable<String> value);

    protected abstract Observable<Boolean> doDelete(Observable<String> key);
}
