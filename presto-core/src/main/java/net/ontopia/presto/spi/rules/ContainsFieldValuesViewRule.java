package net.ontopia.presto.spi.rules;

import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;

import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainsFieldValuesViewRule extends BooleanViewRule {
    
    private static Logger log = LoggerFactory.getLogger(ContainsFieldValuesViewRule.class);
    
    @Override
    protected boolean getResult(ViewFlag flag, PrestoContext context, ObjectNode config, PrestoView view) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            return containsFieldValue(topic, config);
        }
    }

    private boolean containsFieldValue(PrestoTopic topic, ObjectNode config) {
        PrestoField valueField = ContainsFieldValues.getValueField(getSchemaProvider(), topic, config);
        if (valueField != null) {
            Set<String> testValues = ContainsFieldValues.getTestValues(config);
            List<? extends Object> fieldValues = topic.getValues(valueField);
            return ContainsFieldValues.containsAllValues(fieldValues, testValues);
        } else {
            log.warn("Not able to find field from configuration: " + config);
            return false;
        }
    }
    
}