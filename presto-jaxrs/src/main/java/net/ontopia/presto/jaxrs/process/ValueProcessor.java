package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class ValueProcessor extends AbstractProcessor {

    public abstract String getName(PrestoContext context, PrestoFieldUsage field, String value);

    public abstract String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic value);

}
