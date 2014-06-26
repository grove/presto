package net.ontopia.presto.spi.jackson;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.resolve.PrestoResolver;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public abstract class JacksonDataProvider implements DefaultDataProvider {
    
    protected final ObjectMapper mapper;
    protected final JacksonDataStrategy dataStrategy;
    protected final JacksonDataStrategy inlineDataStrategy;
    protected final IdentityStrategy identityStrategy;
    protected final PrestoResolver resolver;
    
    protected JacksonDataProvider() {
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);
        this.inlineDataStrategy = createInlineDataStrategy(mapper);
        this.identityStrategy = createIdentityStrategy();
        this.resolver = createResolver();
    }

    protected ObjectMapper createObjectMapper() {
        return Utils.DEFAULT_OBJECT_MAPPER;
    }

    abstract protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper);

    protected JacksonDataStrategy createInlineDataStrategy(ObjectMapper mapper) {
        return new JacksonDefaultDataStrategy();
    }

    protected JacksonDataStrategy getInlineDataStrategy() {
        return inlineDataStrategy;
    }
    
    protected IdentityStrategy getIdentityStrategy() {
        return identityStrategy;
    }
    
    protected abstract IdentityStrategy createIdentityStrategy();
    
    protected PrestoResolver createResolver() {
        return new PrestoResolver() {
            @Override
            protected JacksonDataProvider getDataProvider() {
                return JacksonDataProvider.this;
            }
        };
    }

    // -- JacksonDataProvider
    
    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    public JacksonDataStrategy getDataStrategy() {
        return dataStrategy;
    }
    
    protected JacksonTopic existing(ObjectNode doc) {
        return doc == null ? null : new JacksonTopic(this, doc);
    }

    @Override
    public DefaultTopic newInstance(PrestoType type, String topicId) {
        return new JacksonTopic(this, createObjectNode(type, topicId));
    }

    public ObjectNode createObjectNode(PrestoType type, String topicId) {
        ObjectNode doc = getObjectMapper().createObjectNode();
        if (topicId != null) {
            doc.put("_id", topicId);
        }
        doc.put(":type", type.getId());
        return doc;
    }

    @Override
    public PrestoChangeSet newChangeSet() {
        return new JacksonChangeSet(this, null);
    }

    @Override
    public PrestoChangeSet newChangeSet(ChangeSetHandler handler) {
        return new JacksonChangeSet(this, handler);
    }


    @Override
    public PrestoInlineTopicBuilder createInlineTopic(PrestoType type, String topicId) {
        return new JacksonInlineTopicBuilder(this, type, topicId);
    }

    private static final class JacksonChangeSet extends PrestoDefaultChangeSet {
        
        public JacksonChangeSet(JacksonDataProvider dataProvider, ChangeSetHandler handler) {
            super(dataProvider, handler);
        }

    }
    
    @Override
    public void updateBulk(List<Change> changes) {
        for (Change c : changes) {
            PrestoTopic topic = c.getTopic();
            if (topic.isInline()) {
                throw new RuntimeException("Cannot save inline topic directly: " + topic);
            }
            switch (c.getType()) {
            case CREATE:
                create(topic);
                break;
            case UPDATE:
                update(topic);
                break;
            case DELETE:
                delete(topic);
                break;
            }
        }
    }

    protected final int compareComparables(String o1, String o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }
    
    @Override
    public Object deserializeFieldValue(PrestoField field, Object value) {
       return value; 
    }

    @Override
    public Object serializeFieldValue(PrestoField field, Object value) {
       return value; 
    }

    @Override
    public List<? extends Object>  resolveValues(PrestoTopic topic, PrestoField field) {
        return resolver.resolveValues(topic, field);
    }

    @Override
    public PagedValues resolveValues(PrestoTopic topic, PrestoField field, Projection projection) {
        return resolver.resolveValues(topic, field, projection);
    }

    @Override
    public PagedValues resolveValues(Collection<? extends Object> objects, PrestoField field, Projection projection, 
            JsonNode resolveConfig, PrestoVariableResolver variableResolver) {
        return resolver.resolveValues(objects, field, projection, resolveConfig, variableResolver);
    }

}
