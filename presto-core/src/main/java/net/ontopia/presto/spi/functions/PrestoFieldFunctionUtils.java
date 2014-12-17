package net.ontopia.presto.spi.functions;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.resolve.PrestoResolver;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PrestoFieldFunctionUtils {

    private static Logger log = LoggerFactory.getLogger(PrestoFieldFunctionUtils.class);

    public static PrestoFieldFunction createFieldFunction(PrestoResolver resolver, 
            PrestoAttributes attributes, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode handlerNode = extra.path("function");
            if (handlerNode.isObject()) {
                PrestoDataProvider dataProvider = resolver.getDataProvider();
                PrestoSchemaProvider schemaProvider = resolver.getSchemaProvider();
                PrestoFieldFunction handler = AbstractHandler.getHandler(dataProvider, schemaProvider, PrestoFieldFunction.class, (ObjectNode)handlerNode);
                if (handler != null) {
                    handler.setAttributes(attributes);
//                    handler.setPresto(this);
                    return handler;
                }
                log.warn("Not able to extract function instance from field " + field.getId() + ": " + extra);                    
            } else if (!handlerNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.function is not an object: " + extra);
            }
        }
        return null;
    }

}
