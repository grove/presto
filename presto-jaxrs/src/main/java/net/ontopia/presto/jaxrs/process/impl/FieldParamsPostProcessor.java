package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.node.ObjectNode;

public class FieldParamsPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        ObjectNode extraNode = getPresto().getFieldExtraNode(field);
        if (extraNode != null) {
            Map<String, Object> params = getPresto().getExtraParamsMap(extraNode);
            if (params != null) {
                fieldData.setParams(params);
            }
        }
        return fieldData;
    }

}
