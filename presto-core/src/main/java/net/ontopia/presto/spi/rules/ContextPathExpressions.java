package net.ontopia.presto.spi.rules;

import java.util.Iterator;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextField;

public class ContextPathExpressions {

    public static PrestoContextField getContextField(PrestoContext context, String path) {
        if (path.charAt(0) == '$') {
            return getContextFieldByExpression(context, path);
        } else {
            return getContextFieldByField(context, path);
        }
    }

    private static PrestoContextField getContextFieldByField(PrestoContext context, String fieldId) {
        PrestoType type = context.getType();
        PrestoField valueField = type.getFieldById(fieldId);
        return new PrestoContextField(context, valueField);
    }

    private static PrestoContextField getContextFieldByExpression(PrestoContext context, String expr) {
        Iterator<String> path = PathExpressions.parsePath(expr).iterator();

        if (path.hasNext()) {
            return getContextFieldByExpression(context, path, expr);
        } else {
            throw new RuntimeException("Not able to extract context from path expression: " + expr);
        }
    }

    private static PrestoContextField getContextFieldByExpression(PrestoContext context, Iterator<String> path, String expr) {

        String p = path.next();
        boolean hasNext = path.hasNext();

        PrestoType type = context.getType();
        if (hasNext) {

            if (":parent".equals(p)) {
                PrestoContext nextContext = context.getParentContext();
                if (nextContext == null) {
                    throw new RuntimeException("Missing parent context from expression: " + expr);
                }
                return getContextFieldByExpression(nextContext, path, expr);
            } else {
                PrestoField valueField = type.getFieldById(p);
                return new PrestoContextField(context, valueField);
            }
            
        } else {
            PrestoField valueField = type.getFieldById(p);
            return new PrestoContextField(context, valueField);
        }
    }

}
