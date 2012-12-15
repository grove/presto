package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.process.AbstractProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public abstract class AvailableFieldCreateTypesResolver extends AbstractProcessor {

    public abstract Collection<PrestoType> getAvailableFieldCreateTypes(PrestoTopic topic, PrestoFieldUsage field);

}
