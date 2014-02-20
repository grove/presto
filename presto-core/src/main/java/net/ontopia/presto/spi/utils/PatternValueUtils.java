package net.ontopia.presto.spi.utils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PatternValueUtils {

    private static final Pattern PATTERN = Pattern.compile("\\$\\{([\\:\\.\\-\\w]+)\\}");

    public static String getValueByPattern(PrestoVariableResolver variableResolver, Object value, ObjectNode config) {
        if (config != null) {
            JsonNode patternNode = config.path("pattern");
            if (patternNode.isTextual()) {
                String pattern = patternNode.getTextValue();
                return getValueByPattern(variableResolver, value, pattern);
            }
        }
        return null;
    }

    public static String getValueByPattern(PrestoSchemaProvider schemaProvider, PrestoContext context, String pattern) {
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(schemaProvider, context);
        PrestoTopic topic = context.getTopic();
        return getValueByPattern(variableResolver, topic, pattern);
    }
    
    private static String getValueByPattern(PrestoVariableResolver variableResolver, Object value, String pattern) {
        String result = pattern;
        Matcher matcher = PATTERN.matcher(pattern);
        while (matcher.find()) {
            String variable = matcher.group(1);
            String replacement = getValue(variableResolver, value, variable);
            result = result.replaceFirst("\\$\\{" + variable + "\\}", replacement);
        }
        return result;
    }

    private static String getValue(PrestoVariableResolver variableResolver, Object value, String variable) {
        List<? extends Object> values = variableResolver.getValues(value, variable);
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Object v = values.get(i);
            if (v instanceof PrestoTopic) {
                sb.append(((PrestoTopic)v).getId());
            } else {
                sb.append(v);
            }
        }
        return sb.toString();
    }

}
