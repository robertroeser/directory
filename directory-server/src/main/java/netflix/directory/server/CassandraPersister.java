package netflix.directory.server;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.netflix.datastax.ClusterManager;
import com.netflix.datastax.SessionObservable;
import rx.Observable;

/**
 * Created by rroeser on 5/28/15.
 */
public class CassandraPersister implements Persister {
    private ClusterManager clusterManager;

    private  SessionObservable sessionObservable;

    private int numberOfBuckets = 10_000;

    public CassandraPersister(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.sessionObservable = clusterManager
            .getSession("directory", ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_ONE);
    }

    @Override
    public Observable<String> get(Observable<String> key) {

        return sessionObservable
            .query(
                Observable.just("select value from directory.index where partition_id = ? and hash_key = ?"),
                key.flatMapIterable(k -> {
                    long partionId = Longs.fromByteArray(k.getBytes());
                    return Lists.newArrayList(partionId, k);
                })
            )
            .map(resultSet ->
                resultSet.one())
            .filter(row -> row != null && !row.isNull(0))
            .map(row ->
                row.getString(0)
            );
    }

    @Override
    public Observable<Boolean> put(Observable<String> key, Observable<String> value) {
        return sessionObservable
            .query(
                Observable.just("insert into directory.index values (?, ?"),
                key.flatMapIterable(k -> {
                    long partionId = Longs.fromByteArray(k.getBytes());
                    return Lists.newArrayList(partionId, k);
                })
            )
            .map(r -> true);
    }

    @Override
    public Observable<Boolean> delete(Observable<String> key) {
        return Observable.just(true);
    }
}
