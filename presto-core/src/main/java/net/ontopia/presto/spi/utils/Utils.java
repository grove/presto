package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    public static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

    private static Logger log = LoggerFactory.getLogger(Utils.class);

    public static boolean different(Object o1, Object o2) {
        return !(o1 == null ? o2 == null : o1.equals(o2));
    }

    public static boolean equals(Object o1, Object o2) {
        return (o1 == null ? o2 == null : o1.equals(o2));
    }

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

    public static void validateNotNull(Object o) {
        if (o == null) {
            throw new NullPointerException("Object cannot be null");
        }
    }
    
    public static String getName(Object o) {
        if (o instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)o;
            return topic.getName();
        } else {
            return o == null ? "null" : o.toString();
        }
    }
 
    public static String getName(PrestoFieldUsage field, Object o) {
        if (o instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)o;
            return topic.getName(field);
        } else {
            return o == null ? "null" : o.toString();
        }
    }

    public static PrestoType getTopicType(PrestoTopic topic, PrestoSchemaProvider schemaProvider) {
        String typeId = topic.getTypeId();
        return schemaProvider.getTypeById(typeId);
    }
    
}
