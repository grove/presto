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
        ObjectNode extraNode = (ObjectNode)field.getExtra();
        if (extraNode != null) {
            int minLength = getInt(extraNode, "minLength");
            int maxLength = getInt(extraNode, "maxLength");
            for (Value value : fieldData.getValues()) {
                String v = value.getValue();
                int length = v.length();
                if (minLength > 0 && minLength > 0) {
                    if (length < minLength || length > maxLength) {
                        setValid(false);
                        if (minLength == maxLength) {
                            addError(fieldData, "Field value must have exactly " + minLength + getCharactersString(minLength));
                        } else {
                            addError(fieldData, "Field value must have between " + minLength + " and " + maxLength + getCharactersString(maxLength));
                        }
                    }
                } else {
                    if (length < minLength) {
                        setValid(false);
                        addError(fieldData, "Field value must have at least " + minLength + getCharactersString(minLength));
                    }
                    if (length > maxLength) {
                        setValid(false);
                        addError(fieldData, "Field value must have at most " + maxLength + getCharactersString(maxLength));
                    }
                }
            }
        }
        return fieldData;
    }

    protected String getCharactersString(int length) {
        if (length == 1) {
            return " character";
        } else {
            return " characters";
        }
    }

    private int getInt(ObjectNode extraNode, String name) {
        JsonNode node = extraNode.path(name);
        if (node.isInt()) {
            return node.getIntValue();
        }
        return 0;
    }

}
