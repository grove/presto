package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class ValueProcessor extends AbstractProcessor {

    public String getName(PrestoContext context, PrestoFieldUsage field, String value) {
        return null;
    }

    public String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic value) {
        return value.getName(field);
    }

    public String getValue(PrestoContext context, PrestoFieldUsage field, String value) {
        return value;
    }

    public String getValue(PrestoContext context, PrestoFieldUsage field, PrestoTopic value) {
        return value.getId();
    }

}
