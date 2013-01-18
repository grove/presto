package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ValueLengthValidator extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {
        ObjectNode processorConfig = (ObjectNode)getConfig();
        if (processorConfig != null) {
            int minLength = getInt(processorConfig, "minLength", 0);
            int maxLength = getInt(processorConfig, "maxLength", 0);
            for (Value value : fieldData.getValues()) {
                String v = value.getValue();
                int length = v.length();
                if (minLength > 0 && maxLength > 0) {
                    if (length < minLength || length > maxLength) {
                        setValid(false);
                        if (minLength == maxLength) {
                            addError(fieldData, getErrorMessage("value-length-exact", field, "Field value must have exactly {0,choice,0#{0} characters|1#{0} character|2#{0} characters}", minLength, maxLength));
                        } else {
                            addError(fieldData, getErrorMessage("value-length-between", field, "Field value must have between {0} and {1} characters", minLength, maxLength));
                        }
                    }
                } else {
                    if (minLength > 0 && length < minLength) {
                        setValid(false);
                        addError(fieldData, getErrorMessage("value-length-at-least", field, "Field value must have at least {0,choice,0#{0} characters|1#{0} character|2#{0} characters}", minLength, maxLength));
                    }
                    if (maxLength > 0 && length > maxLength) {
                        setValid(false);
                        addError(fieldData, getErrorMessage("value-length-no-more-than", field, "Field value must have no more than {1,choice,0#{1} characters|1#{1} character|2#{1} characters}", minLength, maxLength));
                    }
                }
            }
        }
        return fieldData;
    }

    private int getInt(ObjectNode extraNode, String name, int defaultValue) {
        JsonNode node = extraNode.path(name);
        if (node.isInt()) {
            return node.getIntValue();
        }
        return defaultValue;
    }

}
