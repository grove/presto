package net.ontopia.presto.spi.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.functions.PrestoFieldFunction;
import net.ontopia.presto.spi.functions.PrestoFieldFunctionUtils;
import net.ontopia.presto.spi.resolve.PrestoResolver;
import net.ontopia.presto.spi.utils.PrestoAttributes;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class PathExpressions {

    private static final Pattern PATTERN = Pattern.compile("^\\$\\{([\\:\\.\\-\\w]+)\\}$");

    public static List<? extends Object> getValues(PrestoContextRules rules, String path) {
        if (path.charAt(0) == '$') {
            return getValuesByExpression(rules, path);
        } else {
            return getValuesByField(rules, path);
        }
    }

    private static List<? extends Object> getValuesByField(PrestoContextRules rules, String fieldId) {
        PrestoContext context = rules.getContext();
        PrestoType type = context.getType();
        PrestoField field = type.getFieldById(fieldId);
        return rules.getFieldValues(field).getValues();
    }

    private static List<? extends Object> getValuesByExpression(PrestoContextRules rules, String expr) {
        Iterator<String> path = parsePath(expr).iterator();

        if (path.hasNext()) {
            PrestoAttributes attributes = rules.getAttributes();
            PrestoContext context = rules.getContext();
            return getValuesByExpression(context.getResolver(), attributes, Collections.singletonList(context), path, expr);
        } else {
            return Collections.emptyList();
        }
    }

    private static List<? extends Object> getValuesByExpression(PrestoResolver resolver, 
            PrestoAttributes attributes, List<PrestoContext> contexts, Iterator<String> path, String expr) {

        if (contexts.isEmpty()) {
            return Collections.emptyList();
        }
        String p = path.next();
        boolean hasNext = path.hasNext();

        if (hasNext) {
            List<PrestoContext> nextContexts = new ArrayList<PrestoContext>();

            for (PrestoContext context : contexts) {
                PrestoType type = context.getType();

                if (":parent".equals(p)) {
                    PrestoContext nextContext = context.getParentContext();
                    if (nextContext == null) {
                        throw new RuntimeException("Missing parent context from expression: " + expr);
                    }
                    nextContexts.add(nextContext);
                } else if (p.startsWith("#")) {
                    if (!context.isNewTopic()) {
                        PrestoField valueField = type.getFieldById(p.substring(1));
                        PrestoTopic topic = context.getTopic();
                        for (Object value : topic.getStoredValues(valueField)) {
                            if (value instanceof PrestoTopic) {
                                PrestoTopic valueTopic = (PrestoTopic)value;
                                nextContexts.add(PrestoContext.createSubContext(context, valueField, valueTopic));
                            }
                        }
                    }
                } else {
                    PrestoField valueField = type.getFieldById(p);
                    PrestoFieldFunction function = PrestoFieldFunctionUtils.createFieldFunction(resolver, attributes, valueField);
                    List<? extends Object> fieldValues  = null;
                    if (function != null) {
                        fieldValues = function.execute(context, valueField, null);
                    } else if (!context.isNewTopic()) {
                        fieldValues = context.resolveValues(valueField);
                    }
                    if (fieldValues != null) {
                        for (Object value : fieldValues) {
                            if (value instanceof PrestoTopic) {
                                PrestoTopic valueTopic = (PrestoTopic)value;
                                nextContexts.add(PrestoContext.createSubContext(context, valueField, valueTopic));
                            }
                        }
                    }
                }
            }
            
            return getValuesByExpression(resolver, attributes, nextContexts, path, expr);
        } else {

            List<Object> values = new ArrayList<Object>();
            
            for (PrestoContext context : contexts) {
                PrestoType type = context.getType();

                boolean isNew = context.isNewTopic();
                PrestoTopic topic = context.getTopic();

                if (!isNew && p.equals(":id")) {
                    values.add(topic.getId());                
                } else if (!isNew && p.equals(":name")) {
                    values.add(topic.getName());                
                } else if (!isNew && p.startsWith("#")) {
                    PrestoField valueField = type.getFieldById(p.substring(1));
                    for (Object value : topic.getStoredValues(valueField)) {
                        values.add(value);
                    }
                } else if (p.equals(":type")) {
                    values.add(type.getId());                
                } else if (p.equals(":type-name")) {
                    values.add(type.getName());
                } else {
                    PrestoField field = type.getFieldById(p);
                    PrestoFieldFunction function = PrestoFieldFunctionUtils.createFieldFunction(resolver, attributes, field);
                    if (function != null) {
                        values.addAll(function.execute(context, field, null));
                    } else {
                        if (!isNew) {
                            values.addAll(context.resolveValues(field));
                        }
                    }
                }
            }
            return values;
        }
    }

    static List<String> parsePath(String expr) {
        Matcher matcher = PATTERN.matcher(expr);
        if (matcher.find()) {
            String path = matcher.group(1);
            List<String> result = new ArrayList<String>(3);
            int prev = 0;
            int next = path.indexOf('.', prev);
            while (next != -1) {
                String fieldId = path.substring(prev, next);
                result.add(fieldId);
                prev = next + 1;
                next = path.indexOf('.', prev);
            }
            String fieldId = path.substring(prev);
            result.add(fieldId);
            return result;
        } else {
            throw new RuntimeException("Invalid path expression: " + expr);
        }
    }

    public static void main(String[] args) {
        String expr = "${partial_runs.blah.has_run.foo}";
        System.out.println("X: " + parsePath(expr));
    }

}
