package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.jaxrs.process.impl.PatternValueUtils;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;

public class PatternSortKeyGenerator extends SortKeyGenerator {

    @Override
    public String getSortKey(PrestoFieldUsage field, Object value) {
        if (value instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic) value;
            PrestoTopicFieldVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(getSchemaProvider());
            return PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
        } else {
            return null;
        }
    }

}
