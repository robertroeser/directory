package netflix.directory.server;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.reactivex.netty.RxNetty;
import netflix.directory.core.protocol.RequestType;
import netflix.directory.core.protocol.ResponseCode;
import netflix.directory.core.serialization.RequestMessageReader;
import netflix.directory.core.serialization.ResponseMessageWriter;
import rx.Observable;

import java.nio.charset.Charset;

/**
 * Created by rroeser on 5/28/15.
 */
public class Directory {
    public static void main(String... args) {
        final Persister persister = new MapDBCachePersister(new NoopPersister(), 1_000_000);
        final HashFunction hashFunction = Hashing.murmur3_128();

        RxNetty.createTcpServer(7170, handler ->
            handler
                .getInput()
                .flatMap(buf -> {
                    try {
                        RequestMessageReader rmr = RequestMessageReader.getInstance();
                        byte[] bytes = new byte[buf.capacity()];
                        buf.getBytes(0, bytes, 0, bytes.length);
                        rmr.decode(bytes);

                        RequestType requestType = rmr.getRequestMessageDecoder().type();
                        String key = rmr.getRequestMessageDecoder().key();
                        String value = rmr.getRequestMessageDecoder().value();

                        HashCode hashedKey = hashFunction.hashString(key, Charset.defaultCharset());

                        Observable<Void> response = null;

                        System.out.println("Receiving Request " + requestType + " for key " + hashedKey);

                        switch (requestType) {
                            case GET:
                                response = persister
                                    .get(Observable.just(hashedKey.toString()))
                                    .flatMap(fromCache -> {
                                        String v = new String(fromCache);
                                        ResponseMessageWriter rmw = ResponseMessageWriter.getInstance();
                                        rmw.encode(ResponseCode.SUCCESS, v);

                                        System.out.println("Get a key from the directory " + hashedKey);

                                        return handler.writeBytesAndFlush(rmw.getBytes());
                                    });
                                break;
                            case PUT:
                                response = persister
                                    .put(Observable.just(hashedKey.toString()), Observable.just(value))
                                    .flatMap(fromCache -> {
                                        ResponseMessageWriter rmw = ResponseMessageWriter.getInstance();
                                        rmw.encode(ResponseCode.SUCCESS, "ok");

                                        System.out.println("Adding a key to the directory " + hashedKey);

                                        return handler.writeBytesAndFlush(rmw.getBytes());
                                    });
                                break;
                            case DELETE:
                                response = persister.delete(Observable.just(hashedKey.toString()))
                                    .flatMap(fromCache -> {
                                        ResponseMessageWriter rmw = ResponseMessageWriter.getInstance();
                                        rmw.encode(ResponseCode.SUCCESS, "ok");

                                        return handler.writeBytesAndFlush(rmw.getBytes());
                                    });
                                break;
                            default:
                                return Observable.error(new IllegalStateException("Unsupported type: " + requestType));
                        }


                        return response;
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                    return null;
                })
            .doOnError(t -> t.printStackTrace())
        ).startAndWait();
    }


}
