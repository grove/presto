package net.ontopia.presto.spi.resolve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class ValuePatternResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {
        List<Object> result = new ArrayList<Object>();
        if (objects.isEmpty()) {
            String value = PatternValueUtils.getValueByPattern(variableResolver, null, getConfig());
            if (value != null) {
                result.add(value);
            }
            
        } else {
            for (Object o : objects) {
                String value = PatternValueUtils.getValueByPattern(variableResolver, o, getConfig());
                if (value != null) {
                    result.add(value);
                }
            }
        }
        if (!result.isEmpty()) {
            return new PrestoPagedValues(result, paging, 0);
        } else {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }

}
