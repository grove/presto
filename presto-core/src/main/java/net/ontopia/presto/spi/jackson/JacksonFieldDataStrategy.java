package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class JacksonFieldDataStrategy {
    
    private JacksonBucketDataStrategy dataStrategy;
    private ObjectNode config;

    public void setJacksonDataStrategy(JacksonDataStrategy dataStrategy) {
        this.dataStrategy = (JacksonBucketDataStrategy)dataStrategy;
    }

    public JacksonBucketDataStrategy getJacksonDataStrategy() {
        return dataStrategy;
    }
    
    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }
    
    public abstract ArrayNode getFieldValue(ObjectNode doc, PrestoField field);

}
