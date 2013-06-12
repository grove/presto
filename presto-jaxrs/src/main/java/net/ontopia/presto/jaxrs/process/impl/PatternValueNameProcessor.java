package net.ontopia.presto.jaxrs.process.impl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.ValueProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PatternValueNameProcessor extends ValueProcessor {

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, String value) {
        return value;
    }

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic topic) {
        String name = topic.getName(field);
        ObjectNode config = getConfig();
        if (config != null) {
            JsonNode patternNode = config.path("pattern");
            if (patternNode.isTextual()) {
                String pattern = patternNode.getTextValue();
                Pattern p = Pattern.compile("\\$\\{(\\w+)\\}");
                if (name != null) {
                    name = replacePattern(p, pattern, name, topic);
                }
            }
        }
        return name;
    }

    private String replacePattern(Pattern p, String pattern, String name, PrestoTopic topic) {
        String result = pattern;
        Matcher matcher = p.matcher(pattern);
        while (matcher.find()) {
            String variable = matcher.group(1);
            String replacement = getValue(topic, variable);
            result = result.replaceFirst("\\$\\{" + variable + "\\}", replacement);
        }
        return result;
    }

    private String getValue(PrestoTopic topic, String variable) {
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoTopicFieldVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(schemaProvider);
        List<String> values = variableResolver.getValues(topic, variable);
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(values.get(i));
        }
        return sb.toString();
    }

}
