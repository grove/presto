package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class PatternSortKeyGenerator extends SortKeyGenerator {

    @Override
    public String getSortKey(PrestoContext context, PrestoField field, Object value) {
        if (value instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic) value;
            PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
            return PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
        } else if (value != null) {
            return value.toString();
        } else {
            return null;
        }
    }

}
