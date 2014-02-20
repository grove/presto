package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValuesTypeRule extends BooleanTypeRule {
    
    @Override
    protected boolean getResult(TypeFlag flag, PrestoContext context, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            return ContainsFieldValues.containsFieldValue(getDataProvider(), getSchemaProvider(), context, config);
        }
    }
    
}