package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class FieldDataProcessor extends AbstractProcessor {
    
    public abstract FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field);

}
