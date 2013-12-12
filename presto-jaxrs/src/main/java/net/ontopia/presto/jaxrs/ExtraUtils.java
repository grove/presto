package net.ontopia.presto.jaxrs;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ExtraUtils {

    public static ObjectNode getSchemaExtraNode(PrestoSchemaProvider schemaProvider) {
        Object e = schemaProvider.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public static ObjectNode getTypeExtraNode(PrestoType type) {
        Object e = type.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public static ObjectNode getFirstExtraNode(PrestoType type, PrestoView view) {
        ObjectNode extraNode = getViewExtraNode(view);
        if (extraNode == null) {
            extraNode = getTypeExtraNode(type);
            if (extraNode == null) {
                PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
                extraNode = getSchemaExtraNode(schemaProvider);
            }
        }
        return extraNode;
    }

    public static ObjectNode getFirstExtraNode(PrestoType type, PrestoView view, PrestoField field) {
        ObjectNode extraNode = getFieldExtraNode(field);
        if (extraNode == null) {
            extraNode = getFirstExtraNode(type, view);
        }
        return extraNode;
    }
    
    public static ObjectNode getViewExtraNode(PrestoView view) {
        Object e = view.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public static ObjectNode getFieldExtraNode(PrestoField field) {
        Object e = field.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public static String getExtraParamsStringValue(ObjectNode extra, String paramKey) {
        JsonNode params = extra.path("params");
        if (params.isObject()) {
            JsonNode paramNode = params.path(paramKey);
            if (paramNode.isTextual()) {
                return paramNode.getTextValue();
            }
        }
        return null;
    }
    
    public static Map<String,Object> getExtraParamsMap(ObjectNode extra) {
        JsonNode params = extra.path("params");
        if (params.isObject()) {
            Map<String,Object> result = new LinkedHashMap<String,Object>();
            Iterator<String> pnIter = params.getFieldNames();
            while (pnIter.hasNext()) {
                String pn = pnIter.next();
                result.put(pn, params.get(pn));
            }
            return result;
        }
        return null;
    }

}
