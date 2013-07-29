package net.ontopia.presto.jaxrs.process.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ValueRegexValidator extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        ObjectNode processorConfig = (ObjectNode)getConfig();
        if (processorConfig != null) {
            String regexp = getString(processorConfig, "regexp", null);
            Pattern pattern = Pattern.compile(regexp);
            
            for (Value value : fieldData.getValues()) {
                String v = value.getValue();
                Matcher matcher = pattern.matcher(v);
                if (!matcher.matches()) {
                    setValid(false);
                    addError(fieldData, getErrorMessage("value-regexp-not-matching", field, "Field value ''{0}'' does not match expression ''{1}''", v, regexp));
                }
            }
        }
        return fieldData;
    }
    
    private String getString(ObjectNode extraNode, String name, String defaultValue) {
        JsonNode node = extraNode.path(name);
        if (node.isTextual()) {
            return node.getTextValue();
        }
        return defaultValue;
    }

}
