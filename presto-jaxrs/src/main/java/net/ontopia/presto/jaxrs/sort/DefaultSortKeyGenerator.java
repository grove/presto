package net.ontopia.presto.jaxrs.sort;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.FieldValues;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultSortKeyGenerator extends SortKeyGenerator {

    @Override
    public Comparator<Object> getComparator(final PrestoContextRules rules, final PrestoField field, final Projection projection) {
        if (projection != null && projection.isSorted() && field.isReferenceField()) {
            return new FieldProjectionComparator(getPresto(), rules, field, projection);
        } else {
            return new FieldComparator(field, rules.isSortedAscendingField(field));
        }
    }

    private static final class FieldComparator implements Comparator<Object> {
        private final PrestoField field;
        private final boolean ascending;

        private FieldComparator(PrestoField field, boolean ascending) {
            this.field = field;
            this.ascending = ascending;
        }

        @Override
        public int compare(Object o1, Object o2) {
            String n1 = getSortKey(o1);
            String n2 = getSortKey(o2);
            return Utils.compareComparables(n1, n2, ascending);
        }

        private String getSortKey(Object value) {
            if (value instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)value;
                return topic.getName(field);
            } else {
                return value == null ? null : value.toString();
            }
        }
    }

    private static final class FieldProjectionComparator implements Comparator<Object> {
        private final PrestoContextRules rules;
        private final PrestoField field;
        @SuppressWarnings("unused")
        private final Projection projection;
        
        private final boolean ascending;
        private final String orderField;
        private final Presto presto;
        
        private FieldProjectionComparator(Presto presto, PrestoContextRules rules, PrestoField field, Projection projection) {
            this.presto = presto;
            this.rules = rules;
            this.field = field;
            this.projection = projection;
            
            boolean isAscending = rules.isSortedAscendingField(field);
            String orderFieldId = null;
            String orderBy = projection.getOrderBy();
            if (orderBy != null) {
                String[] split = orderBy.split(" ");
                orderFieldId = split[0];
                String orderDirection = split[1];
                if (orderDirection != null) {
                    isAscending = !orderDirection.equals("desc");
                }
            }
            this.ascending = isAscending;
            this.orderField = orderFieldId;
        }

        @Override
        public int compare(Object o1, Object o2) {
            PrestoTopic t1 = (PrestoTopic)o1;
            PrestoTopic t2 = (PrestoTopic)o2;
            ValueFieldConfig vfc1 = getValueFieldConfig(t1, orderField);
            if (ascending) {
                return vfc1.compare(t1, t2);
            } else {
                return vfc1.compare(t1, t2) * -1;
            }
        }

        private static class ValueFieldConfig {
            private Presto presto;
            private PrestoContextRules rules;
            private PrestoField field;
            private PrestoField valueField;
            private Comparator<Object> valueComparator;

            public ValueFieldConfig(Presto presto, PrestoContextRules rules, PrestoField field, PrestoField valueField, Comparator<Object> valueComparator) {
                this.presto = presto;
                this.rules = rules;
                this.field = field;
                this.valueField = valueField;
                this.valueComparator = valueComparator;
            }
            public int compare(PrestoTopic t1, PrestoTopic t2) {
                Object v1 = getValue(t1);
                Object v2 = getValue(t2);
                return valueComparator.compare(v1, v2);
            }
            private Object getValue(PrestoTopic valueTopic) {
                PrestoContextRules subrules = getValueTopicContextRules(valueTopic);

                FieldValues fieldValues = presto.getFieldValues(subrules, valueField);
              
                List<? extends Object> values = fieldValues.getValues();
                return values.isEmpty() ? null : values.get(0);
            }
            private PrestoContextRules getValueTopicContextRules(PrestoTopic valueTopic) {
                PrestoContext context = rules.getContext();

                PrestoContext subcontext = PrestoContext.createSubContext(context, field, valueTopic);
                PrestoContextRules subrules = presto.getPrestoContextRules(subcontext);
                return subrules;
            }
        }

        private final Map<String,ValueFieldConfig> valueFieldConfigCache = new HashMap<String,ValueFieldConfig>();
        
        private ValueFieldConfig getValueFieldConfig(PrestoTopic valueTopic, String valueFieldId) {
            String valueTypeId = valueTopic.getTypeId();

            ValueFieldConfig valueFieldConfig = valueFieldConfigCache.get(valueTypeId);
            if (valueFieldConfig == null) {
                
                PrestoDataProvider dataProvider = presto.getDataProvider();
                PrestoSchemaProvider schemaProvider = presto.getSchemaProvider();
                PrestoType valueType = schemaProvider.getTypeById(valueTypeId);
                PrestoField valueField = valueType.getFieldById(valueFieldId);
                
                Comparator<Object> valueComparator = null;
                ObjectNode extra = ExtraUtils.getFieldExtraNode(valueField);
                if (extra != null) {
                    JsonNode handlerNode = extra.path("valueComparator");
                    if (handlerNode.isObject()) {
                        FieldValueComparator handler = AbstractHandler.getHandler(dataProvider, schemaProvider, FieldValueComparator.class, (ObjectNode)handlerNode);
                        if (handler != null) {
                            handler.setPresto(presto);
                        }
                        valueComparator = handler;
                    }
                }
                if (valueComparator == null) {
                    valueComparator = new FieldComparator(valueField, false);
                }
                valueFieldConfig = new ValueFieldConfig(presto, rules, field, valueField, valueComparator);
                valueFieldConfigCache.put(valueTypeId, valueFieldConfig);
            }
            return valueFieldConfig;
        }
    }

}
