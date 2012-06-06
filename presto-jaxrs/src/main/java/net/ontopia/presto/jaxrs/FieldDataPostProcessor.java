package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class FieldDataPostProcessor extends AbstractProcessor {

    public abstract FieldData postProcess(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field);
    
}
