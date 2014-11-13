package net.ontopia.presto.spi.jackson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoLazyTopicBuilder;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.resolve.PrestoResolver;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class JacksonDataProvider implements DefaultDataProvider {
    
    protected final ObjectMapper mapper;
    protected final JacksonDataStrategy dataStrategy;
    protected final JacksonDataStrategy inlineDataStrategy;
    protected final IdentityStrategy identityStrategy;
    protected final PrestoResolver resolver;
    protected final PrestoSchemaProvider schemaProvider;
    
    protected JacksonDataProvider(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);
        this.inlineDataStrategy = createInlineDataStrategy(mapper);
        this.identityStrategy = createIdentityStrategy();
        this.resolver = createResolver();
    }

    protected PrestoResolver createResolver() {
        return new PrestoResolver(this, schemaProvider);
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

    // -- JacksonDataProvider
    
    public PrestoResolver getResolver() {
        return resolver;
    }
    
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

    @Override
    public PrestoLazyTopicBuilder createLazyTopic(PrestoType type, String topicId) {
        return new JacksonLazyTopicBuilder(this, type, topicId);
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
    
    @Override
    public Object deserializeFieldValue(PrestoField field, Object value) {
       return value; 
    }

    @Override
    public Object serializeFieldValue(PrestoField field, Object value) {
       return value; 
    }
    
    public List<? extends Object> resolveValues(PrestoTopic topic, PrestoField field) {
        return resolver.resolveValues(topic, field);
    }

    public PagedValues resolveValues(PrestoTopic topic, PrestoField field, Projection projection) {
        return resolver.resolveValues(topic, field, projection);
    }

    // lazy topics
    protected PrestoTopic lazyLoad(String topicId) {
        PrestoType type = getTypeOfLazyTopic(topicId, schemaProvider);
        if (type != null) {
            return resolver.buildLazyTopic(type, topicId);
        }
        return null;
    }
    
    private static PrestoType getTypeOfLazyTopic(String topicId, PrestoSchemaProvider schemaProvider) {
        int ix = 0;
        while (true) {
            ix = topicId.indexOf(":", ix+1);
            if (ix < 0) {
                break;
            }
            String typeId = topicId.substring(0, ix);
            PrestoType type = schemaProvider.getTypeById(typeId, null);
            if (type != null && type.isLazy()) {
                return type;
            }
        }
        return null;
//        throw new RuntimeException("Not able to extract type id from topic id '" + topicId + "'");
    }

    protected Collection<PrestoTopic> includeLazyTopics(Collection<PrestoTopic> found, Collection<String> topicIds) {
        if (found.size() < topicIds.size()) {
            Collection<PrestoTopic> result = new ArrayList<PrestoTopic>(found);
            Map<String,PrestoTopic> foundIds = new HashMap<String,PrestoTopic>(topicIds.size());
            for (PrestoTopic topic : found) {
                foundIds.put(topic.getId(), topic);
            }
            for (String topicId : topicIds) {                
                if (!foundIds.containsKey(topicId)) {
                    PrestoTopic topic = lazyLoad(topicId);
                    if (topic != null) {
                        result.add(topic);
                    }
                }
            }
            return result;
        } else {
            return found;
        }
    }
    
}
