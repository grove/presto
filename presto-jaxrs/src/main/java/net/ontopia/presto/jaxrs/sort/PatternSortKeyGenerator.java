package net.ontopia.presto.jaxrs.sort;

import java.util.Comparator;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.ontopia.presto.spi.utils.Utils;

public class PatternSortKeyGenerator extends SortKeyGenerator {

    private final class PatternSortKeyComparator implements Comparator<Object> {
        private final PrestoField field;
        private final PrestoContextRules rules;
        private final PrestoVariableResolver variableResolver;
        private final boolean ascending;

        private PatternSortKeyComparator(PrestoField field, PrestoContextRules rules) {
            this.field = field;
            this.rules = rules;
            this.variableResolver = new PrestoTopicWithParentFieldVariableResolver(rules.getContext());
            this.ascending = rules.isSortedAscendingField(field);
        }

        @Override
        public int compare(Object o1, Object o2) {
            PrestoContext context = rules.getContext();
            String n1 = getSortKey(context, field, o1);
            String n2 = getSortKey(context, field, o2);
            return Utils.compareComparables(n1, n2, ascending);
        }

        private String getSortKey(PrestoContext context, PrestoField field, Object value) {
            if (value instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic) value;
                return PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
            } else if (value != null) {
                return value.toString();
            } else {
                return null;
            }                    
        }
    }

    @Override
    public Comparator<Object> getComparator(final PrestoContextRules rules, final PrestoField field, Projection projection) {
        return new PatternSortKeyComparator(field, rules);
    }

}
