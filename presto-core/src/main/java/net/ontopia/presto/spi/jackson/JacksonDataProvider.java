package net.ontopia.presto.spi.jackson;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public abstract class JacksonDataProvider implements DefaultDataProvider {

    protected final ObjectMapper mapper;
    protected final JacksonDataStrategy dataStrategy;
    protected final IdentityStrategy identityStrategy;
    protected final JacksonResolver resolver;
    
    private static final JacksonDataStrategy inlineDataStrategy = new JacksonDefaultDataStrategy();
    
    protected JacksonDataProvider() {
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);
        this.identityStrategy = createIdentityStrategy();
        this.resolver = createResolver();
    }

    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    abstract protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper);
    
    protected IdentityStrategy getIdentityStrategy() {
        return identityStrategy;
    }
    
    protected abstract IdentityStrategy createIdentityStrategy();

    protected JacksonDataStrategy getInlineDataStrategy() {
        return inlineDataStrategy;
    }
    
    protected JacksonResolver createResolver() {
        return new JacksonResolver() {
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

    private static final class JacksonChangeSet extends PrestoDefaultChangeSet {

        private JacksonDataProvider nativeDataProvider;
        
        public JacksonChangeSet(JacksonDataProvider dataProvider, ChangeSetHandler handler) {
            super(dataProvider, handler);
            this.nativeDataProvider = dataProvider;
        }

        @Override
        public PrestoInlineTopicBuilder createInlineTopic(PrestoType type, String topicId) {
            return new JacksonInlineTopicBuilder(nativeDataProvider, type, topicId);
        }

    }
    
    @Override
    public void updateBulk(List<Change> changes) {
        for (Change c : changes) {
            switch (c.getType()) {
            case CREATE:
                create(c.getTopic());
                break;
            case UPDATE:
                update(c.getTopic());
                break;
            case DELETE:
                delete(c.getTopic());
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

    public PagedValues resolveValues(Collection<? extends Object> objects, PrestoField field, Paging paging, 
            JsonNode resolveConfig, PrestoVariableResolver variableResolver) {
        return resolver.resolveValues(objects, field, paging, resolveConfig, variableResolver);
    }

}
