package net.ontopia.presto.jaxrs.sort;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleFieldValueComparator extends FieldValueComparator {

    private static Logger log = LoggerFactory.getLogger(DoubleFieldValueComparator.class);

    private Map<String,Double> defaultValues = new HashMap<String,Double>();
    
    @Override
    public void setConfig(ObjectNode config) {
        super.setConfig(config);
        
        JsonNode defaults = config.path("defaults");
        if (defaults.isObject()) {
            ObjectNode map = (ObjectNode)defaults;
            Iterator<String> iter = map.getFieldNames();
            while (iter.hasNext()) {
                String key = iter.next();
                double value = map.path(key).asDouble();
                defaultValues.put(key, value);
            }
        }
    }
    
    @Override
    public int compare(Object o1, Object o2) {
        Double d1 = getDoubleValue(o1);
        Double d2 = getDoubleValue(o2);
        return d1.compareTo(d2);
    }

    private double getDoubleValue(Object o) {
        Double defaultValue = defaultValues.get(o);
        if (defaultValue != null) {
            return defaultValue;
        }
        if (o instanceof String) {
            try {
                return Double.valueOf((String)o);
            } catch (Exception e) {
                log.warn("Could not cast " + o + " as double.");
            }
        }
        return 0d;
    }

}
