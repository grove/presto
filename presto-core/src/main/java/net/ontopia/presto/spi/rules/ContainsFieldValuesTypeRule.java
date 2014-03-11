package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValuesTypeRule extends BooleanTypeRule {
    
    @Override
    protected boolean getResult(TypeFlag flag, PrestoContextRules rules, ObjectNode config) {
        return ContainsFieldValues.containsFieldValues(getDataProvider(), getSchemaProvider(), rules, config);
    }
    
}