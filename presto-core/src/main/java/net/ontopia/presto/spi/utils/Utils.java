package net.ontopia.presto.spi.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger log = LoggerFactory.getLogger(Utils.class);

    public static final <T> T newInstanceOf(String className, Class<T> type) {
        return newInstanceOf(className, type, true);
    }
    
    @SuppressWarnings("unchecked")
    public static final <T> T newInstanceOf(String className, Class<T> type, boolean warnIfDifferentType) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> klass = Class.forName(className, true, classLoader);
            if (type.isAssignableFrom(klass)) {
                return (T) klass.newInstance();
            } else if (warnIfDifferentType) {
                log.warn("Class " + className + " not assignable to " + type);                    
            }
        } catch (ClassNotFoundException e) {
            log.warn("Class " + className + " not found.");
        } catch (InstantiationException e) {
            log.warn("Not able to instatiate class " + className + ".");
        } catch (IllegalAccessException e) {
            log.warn("Not able to instatiate class " + className + " (illegal access).");
        }
        return null;
    }

}
