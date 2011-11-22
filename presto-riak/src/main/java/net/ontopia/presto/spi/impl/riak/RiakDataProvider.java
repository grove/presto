package net.ontopia.presto.spi.impl.riak;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonFieldStrategy;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoFunctionResolver;
import net.ontopia.presto.spi.utils.PrestoTraverseResolver;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RiakDataProvider implements JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(RiakDataProvider.class.getName());

    private final ObjectMapper mapper;
    private final JacksonFieldStrategy fieldStrategy;

    public RiakDataProvider() {
        this.mapper = createObjectMapper();
        this.fieldStrategy = createFieldStrategy(mapper);  
    }
    
    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    abstract protected JacksonFieldStrategy createFieldStrategy(ObjectMapper mapper);

    // -- PrestoDataProvider
    
    @Override
    public String getProviderId() {
        return "riak";
    }

    @Override
    public PrestoTopic getTopicById(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrestoChangeSet newChangeSet() {
        return new PrestoDefaultChangeSet(this);
    }

    @Override
    public void close() {
    }

    // -- DefaultDataProvider
    
    protected JacksonTopic existing(ObjectNode doc) {
        return doc == null ? null : new JacksonTopic(this, doc);
    }

    @Override
    public DefaultTopic newInstance(PrestoType type) {
        ObjectNode doc = getObjectMapper().createObjectNode();
        doc.put(":type", type.getId());
        return new JacksonTopic(this, doc);
    }

    @Override
    public void create(PrestoTopic topic) {
        // TODO Auto-generated method stub
    }

    @Override
    public void update(PrestoTopic topic) {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void updateBulk(List<Change> changes) {
        // TODO Auto-generated method stub
    }

    // -- JacksonDataProvider
    
    @Override
    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    @Override
    public JacksonFieldStrategy getFieldStrategy() {
        return fieldStrategy;
    }

    @Override
    public PrestoFieldResolver createFieldResolver(PrestoSchemaProvider schemaProvider, ObjectNode config) {
        PrestoContext context = new PrestoContext(this, schemaProvider, getObjectMapper());
        String type = config.get("type").getTextValue();
        if (type == null) {
            log.error("type not specified on resolve item: " + config);
        } else if (type.equals("traverse")) {
            return new PrestoTraverseResolver(context, config);
        } else if (type.equals("function")) {
            return new PrestoFunctionResolver(context, config);
        } else {
            log.error("Unknown type specified on resolve item: " + config);            
        }
        return null;
    }

}
