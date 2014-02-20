package net.ontopia.presto.spi.functions;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrestoFieldFunctionUtils {

    private static Logger log = LoggerFactory.getLogger(PrestoFieldFunctionUtils.class);

    public static PrestoFieldFunction createFieldFunction(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode handlerNode = extra.path("function");
            if (handlerNode.isObject()) {
                PrestoFieldFunction handler = AbstractHandler.getHandler(dataProvider, schemaProvider, PrestoFieldFunction.class, (ObjectNode)handlerNode);
                if (handler != null) {
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
