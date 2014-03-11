package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldValueRule extends BooleanFieldValueRule {

    @Override
    protected boolean getResult(FieldValueFlag flag, PrestoContextRules rules, PrestoField field, Object value, ObjectNode config) {
        if (value instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)value;
            PrestoType type = Utils.getTopicType(topic, getSchemaProvider());
            PrestoView view = field.getValueView(type);
            PrestoContext subContext = PrestoContext.createSubContext(rules.getContext(), field, topic, type, view);
            PrestoContextRules subRules = rules.getPrestoContextRules(subContext);
            return HasFieldValues.hasFieldValues(getDataProvider(), getSchemaProvider(), subRules, config);
        }
        return false;
    }
    
}