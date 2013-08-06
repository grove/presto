package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public class DefaultValuesPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        Collection<Value> fieldValues = fieldData.getValues();

        // don't add values if field already has values
        if ((fieldValues == null || fieldValues.isEmpty()) && context.isNewTopic()) {
            
            ObjectNode config = getConfig();
            if (config != null) {
                List<Object> values = new ArrayList<Object>();
                JsonNode valuesNode = config.path("values");
                if (valuesNode.isArray()) {
                    for (JsonNode valueNode : valuesNode) {
                        String textValue = valueNode.getTextValue();
                        if (field.isReferenceField()) {
                            PrestoTopic topic = getDataProvider().getTopicById(textValue);
                            if (topic != null) {
                                values.add(topic);
                            }
                        } else {
                            values.add(textValue);
                        }
                    }
                }
                int offset = 0;
                int limit = Presto.DEFAULT_LIMIT;
                getPresto().setFieldDataValues(offset, limit, context, field, fieldData, values);
            }
        }
        return fieldData;
    }

}
