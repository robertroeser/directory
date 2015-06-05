package netflix.directory.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by rroeser on 6/3/15.
 */
public interface Loggable {
    default Logger logger() {
        return logger(this.getClass());
    }

    static Logger logger(Class<?> clazz) { return loggers.computeIfAbsent(clazz, LoggerFactory::getLogger); }

    static final ConcurrentHashMap<Class<?>, Logger> loggers = new ConcurrentHashMap<>();
}
