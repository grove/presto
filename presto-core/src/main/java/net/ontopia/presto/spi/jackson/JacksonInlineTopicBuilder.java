package net.ontopia.presto.spi.jackson;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class JacksonInlineTopicBuilder implements PrestoInlineTopicBuilder {

    private JacksonDataProvider dataProvider;
    private PrestoType type;
    private String topicId;
    private Map<PrestoField,Collection<?>> fields = new HashMap<PrestoField,Collection<?>>();
    
    public JacksonInlineTopicBuilder(JacksonDataProvider dataProvider, PrestoType type, String topicId) {
        if (dataProvider == null) {
            throw new RuntimeException("dataProvider is null");
        }
        if (type == null) {
            throw new RuntimeException("type is null");
        }
//        if (topicId == null) {
//            throw new RuntimeException("topicId is null");
//        }
        this.dataProvider = dataProvider;
        this.type = type;
        this.topicId = topicId;
    }

    @Override
    public void setValues(PrestoField field, Collection<?> values) {
        fields.put(field, values);
    }

    @Override
    public PrestoTopic build() {
        if (topicId == null) {
            topicId = dataProvider.getIdentityStrategy().generateId(type.getId(), null);
        }
        ObjectNode data = dataProvider.createObjectNode(type, topicId);
        JacksonInlineTopic result = new JacksonInlineTopic(dataProvider, data);
        for (Entry<PrestoField,Collection<?>> e : fields.entrySet()) {
            PrestoField field = e.getKey();
            Collection<?> values = e.getValue();
            result.setValue(field, values);
        }
        return result;
    }
    
}
