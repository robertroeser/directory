package netflix.directory.server;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import rx.Observable;

/**
 * Created by rroeser on 5/28/15.
 */
public class MapDBCachePersister extends ChainablePersister {
    private HTreeMap<String, String> cache;

    public MapDBCachePersister(Persister child, long size) {
        super(child);

        cache = DBMaker
            .newMemoryDirectDB()
            .transactionDisable()
            .cacheLRUEnable()
            .make()
            .createHashMap("cache")
            .expireMaxSize(size)
            .counterEnable()
            .make();
    }

    @Override
    protected Observable<String> doGet(Observable<String> key) {
        return key
            .map(k ->
                cache.get(k)
            )
            .filter(value ->
                    value != null
            );
    }

    @Override
    protected Observable<Boolean> doPut(Observable<String> key, Observable<String> value) {
        return key
            .zipWith(value, (k, v) -> {
                cache.put(k, v);
                return true;
            });
    }

    @Override
    protected Observable<Boolean> doDelete(Observable<String> key) {
        return key
            .map(cache::remove)
            .map(f -> true);
    }
}
