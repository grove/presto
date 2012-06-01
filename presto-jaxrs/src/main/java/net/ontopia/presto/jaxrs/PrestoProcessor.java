package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;

public abstract class PrestoProcessor {

    public FieldData postProcess(FieldData fieldData, PrestoField field) {
        return fieldData;
    }

    public Topic postProcess(Topic topic, PrestoType type) {
        return topic;
    }
    
}
