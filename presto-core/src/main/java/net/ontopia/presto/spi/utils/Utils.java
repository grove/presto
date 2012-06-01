package net.ontopia.presto.spi.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger log = LoggerFactory.getLogger(Utils.class.getName());

    @SuppressWarnings("unchecked")
    public static final <T> T newInstanceOf(String className, Class<T> type) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> klass = Class.forName(className, true, classLoader);
            if (type.isAssignableFrom(klass)) {
                return (T) klass.newInstance();
            } else {
                log.warn("Function class " + className + " not a PrestoFunction.");                    
            }
        } catch (ClassNotFoundException e) {
            log.warn("Function class " + className + " not found.");
        } catch (InstantiationException e) {
            log.warn("Not able to instatiate function class " + className + ".");
        } catch (IllegalAccessException e) {
            log.warn("Not able to instatiate function class " + className + " (illegal access).");
        }
        return null;
    }

}
