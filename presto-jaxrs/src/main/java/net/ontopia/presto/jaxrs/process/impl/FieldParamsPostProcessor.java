package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.node.ObjectNode;

public class FieldParamsPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoField field, Projection projection) {
        ObjectNode extraNode = ExtraUtils.getFieldExtraNode(field);
        if (extraNode != null) {
            // field params
            Map<String, Object> params = ExtraUtils.getExtraParamsMap(extraNode);
            if (params != null) {
                fieldData.setParams(params);
            }
            // value params
            Map<String, Object> valueParams = ExtraUtils.getParamsMap(extraNode.path("value-params"));
            if (valueParams != null) {
                for (Value v : fieldData.getValues()) {
                    v.setParams(valueParams);
                }
            }
            
        }
        return fieldData;
    }

}
