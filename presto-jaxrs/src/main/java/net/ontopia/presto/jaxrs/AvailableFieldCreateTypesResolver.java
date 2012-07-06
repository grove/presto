package net.ontopia.presto.jaxrs;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public abstract class AvailableFieldCreateTypesResolver extends AbstractProcessor {

    public abstract Collection<PrestoType> getAvailableFieldCreateTypes(PrestoTopic topic, PrestoFieldUsage field);

}
