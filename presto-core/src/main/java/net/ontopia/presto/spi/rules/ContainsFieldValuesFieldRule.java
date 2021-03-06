package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ContainsFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config) {
        return ContainsFieldValues.containsFieldValues(getDataProvider(), getSchemaProvider(), rules, field, config);
    }
    
}