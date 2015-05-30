package netflix.directory.client;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ObservableConnection;
import netflix.directory.core.protocol.RequestType;
import netflix.directory.core.serialization.RequestMessageWriter;
import netflix.directory.core.serialization.ResponseMessageReader;
import rx.Observable;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 5/28/15.
 */
public class DirectoryClient {
    public static void main(String... args) {
        System.out.println("Start Client...");
         RxNetty
             .createTcpClient("localHost", 7170)
             .connect()
             .flatMap(connection ->
                     Observable.interval(1, TimeUnit.SECONDS)
                         .flatMap(i -> {

                             RequestMessageWriter rmw = RequestMessageWriter.getInstance();
                             System.out.println("Adding a key " + "aKey_" + i);
                             rmw.encode(RequestType.PUT, "aKey_" + i, "localhost:7171");
                             Observable<Void> put = connection.writeBytesAndFlush(rmw.getBytes());

                             System.out.println("Get a key " + "aKey_" + i);
                             rmw.encode(RequestType.GET, "aKey_" + i, "");
                             Observable<String> get = connection.writeBytesAndFlush(rmw.getBytes())
                                 .map(v -> "")
                                 .concatWith(readInput(connection));

                            return Observable.concat(put, get);
                         })
             )
             .toBlocking()
             .forEach(System.out::println);

    }

    private static Observable<String> readInput(ObservableConnection<ByteBuf, ByteBuf> connection) {
        return connection.getInput()
            .map(buf -> {
                byte[] bytes = new byte[buf.capacity()];
                buf.getBytes(0, bytes, 0, bytes.length);
                return ResponseMessageReader.getInstance().decode(bytes);
            })
            .filter(pd -> pd.size() > 0) // why can it sometimes be zero?
            .map(pd -> pd.value());
    }
}