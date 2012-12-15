package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.process.AbstractProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class AvailableFieldValuesResolver extends AbstractProcessor {

    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoFieldUsage field);

}
