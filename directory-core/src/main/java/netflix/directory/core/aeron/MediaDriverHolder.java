package netflix.directory.core.aeron;

import uk.co.real_logic.aeron.driver.MediaDriver;

/**
 * Created by rroeser on 6/1/15.
 */
public class MediaDriverHolder {
    private final MediaDriver mediaDriver;
    private final MediaDriver.Context context;

    private static MediaDriverHolder MEDIA_DRIVER_HOLDER = null;

    private MediaDriverHolder() {
        context = new MediaDriver.Context();
        mediaDriver = MediaDriver.launch(context);
    }

    public static synchronized MediaDriverHolder getInstance() {
        if (MEDIA_DRIVER_HOLDER == null) {
            MEDIA_DRIVER_HOLDER = new MediaDriverHolder();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (MEDIA_DRIVER_HOLDER.mediaDriver != null) {
                    MEDIA_DRIVER_HOLDER.mediaDriver.close();
                }
            }));

        }

        return MEDIA_DRIVER_HOLDER;
    }

}
