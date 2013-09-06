package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxrs.PrestoContextRules;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class FieldMessagesPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        ObjectNode extraNode = getPresto().getFieldExtraNode(field);
        if (extraNode != null) {
            String messagesKey = getMessagesKey();
            JsonNode messagesNode = extraNode.path(messagesKey);
            if (messagesNode.isArray()) {
                List<FieldData.Message> messages = new ArrayList<FieldData.Message>();
                for (JsonNode messageNode : messagesNode) {
                    String type = messageNode.get("type").getTextValue();
                    String message = messageNode.get("message").getTextValue();
                    messages.add(new FieldData.Message(type, message));
                }
                if (fieldData.getMessages() != null) {
                    fieldData.getMessages().addAll(messages);
                } else {
                    fieldData.setMessages(messages);
                }
            }
        }
        return fieldData;
    }

    private String getMessagesKey() {
        String result = null;
        ObjectNode config = getConfig();
        if (config != null && config.has("key")) {
            result = config.get("key").getTextValue();
        }
        return result == null ? "messages" : result;
    }

}
