package net.ontopia.presto.spi.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.functions.PrestoFieldFunction;
import net.ontopia.presto.spi.functions.PrestoFieldFunctionUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.Utils;

public class PathExpressions {

    private static final Pattern PATTERN = Pattern.compile("^\\$\\{([\\:\\.\\-\\w]+)\\}$");

    public static List<? extends Object> getValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, String path) {
        if (path.charAt(0) == '$') {
            return getValuesByExpression(dataProvider, schemaProvider, context, path);
        } else {
            return getValuesByField(dataProvider, schemaProvider, context, path);
        }
    }

    private static List<? extends Object> getValuesByField(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, 
            PrestoContext context, String fieldId) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = Utils.getTopicType(topic, schemaProvider);
        PrestoField field = type.getFieldById(fieldId);
        return topic.getValues(field);
    }

    private static List<? extends Object> getValuesByExpression(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, 
            PrestoContext context, String expr) {
        Iterator<String> path = getPath(expr).iterator();
        System.out.println("P: " + path);

        if (path.hasNext()) {
            return getValuesByExpression(dataProvider, schemaProvider, Collections.singletonList(context), path, expr);
        } else {
            return Collections.emptyList();
        }
    }

    private static List<? extends Object> getValuesByExpression(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, 
            List<PrestoContext> contexts, Iterator<String> path, String expr) {

        if (contexts.isEmpty()) {
            return Collections.emptyList();
        }
        String p = path.next();
        boolean hasNext = path.hasNext();

        if (hasNext) {
            List<PrestoContext> nextContexts = new ArrayList<PrestoContext>();

            for (PrestoContext context : contexts) {
                PrestoTopic topic = context.getTopic();
                PrestoType type = context.getType();
                PrestoView view = context.getView();

                if (":parent".equals(p)) {
                    PrestoContext nextContext = context.getParentContext();
                    if (nextContext == null) {
                        throw new RuntimeException("Missing parent context from expression: " + expr);
                    }
                    nextContexts.add(nextContext);
                } else if (p.startsWith("#")) {
                    PrestoFieldUsage valueField = type.getFieldById(p.substring(1), view);
                    for (Object value : topic.getStoredValues(valueField)) {
                        if (value instanceof PrestoTopic) {
                            PrestoTopic valueTopic = (PrestoTopic)value;
                            nextContexts.add(PrestoContext.createSubContext(dataProvider, schemaProvider, context, valueField, valueTopic));
                        }
                    }
                } else {
                    PrestoFieldUsage valueField = type.getFieldById(p, view);
                    PrestoFieldFunction function = PrestoFieldFunctionUtils.createFieldFunction(dataProvider, schemaProvider, valueField);
                    List<? extends Object> fieldValues;
                    if (function != null) {
                        fieldValues = function.execute(context, valueField);
                    } else {
                        fieldValues = topic.getValues(valueField);
                    }
                    for (Object value : fieldValues) {
                        if (value instanceof PrestoTopic) {
                            PrestoTopic valueTopic = (PrestoTopic)value;
                            nextContexts.add(PrestoContext.createSubContext(dataProvider, schemaProvider, context, valueField, valueTopic));
                        }
                    }
                }
            }
            
            return getValuesByExpression(dataProvider, schemaProvider, nextContexts, path, expr);
        } else {

            List<Object> values = new ArrayList<Object>();
            
            for (PrestoContext context : contexts) {
                PrestoTopic topic = context.getTopic();
                PrestoType type = context.getType();
                PrestoView view = context.getView();

                if (p.equals(":id")) {
                    values.add(topic.getId());                
                } else if (p.equals(":name")) {
                    values.add(topic.getName());                
                } else if (p.equals(":type")) {
                    values.add(type.getId());                
                } else if (p.equals(":type-name")) {
                    values.add(type.getName());
                } else if (p.startsWith("#")) {
                    PrestoFieldUsage valueField = type.getFieldById(p.substring(1), view);
                    for (Object value : topic.getStoredValues(valueField)) {
                        values.add(value);
                    }
                } else {
                    PrestoFieldUsage field = type.getFieldById(p, view);
                    PrestoFieldFunction function = PrestoFieldFunctionUtils.createFieldFunction(dataProvider, schemaProvider, field);
                    if (function != null) {
                        values.addAll(function.execute(context, field));
                    } else {
                        values.addAll(topic.getValues(field));
                    }
                }
            }
            return values;
        }
    }

    private static List<String> getPath(String expr) {
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
        System.out.println("X: " + getPath(expr));
    }

}
