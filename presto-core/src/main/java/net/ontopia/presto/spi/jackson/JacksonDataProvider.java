package net.ontopia.presto.spi.jackson;

import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoFunctionResolver;
import net.ontopia.presto.spi.utils.PrestoTraverseResolver;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JacksonDataProvider implements DefaultDataProvider {

    private static Logger log = LoggerFactory.getLogger(JacksonDataProvider.class.getName());

    protected final ObjectMapper mapper;
    protected final JacksonDataStrategy dataStrategy;
    protected final IdentityStrategy identityStrategy;

    protected JacksonDataProvider() {
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);
        this.identityStrategy = createIdentityStrategy();
    }
    
    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    abstract protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper);
    
    protected IdentityStrategy getIdentityStrategy() {
        return identityStrategy;
    }
    
    protected abstract IdentityStrategy createIdentityStrategy();

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
    public DefaultTopic newInstance(PrestoType type) {
        ObjectNode doc = getObjectMapper().createObjectNode();
        doc.put(":type", type.getId());
        return new JacksonTopic(this, doc);
    }

    @Override
    public PrestoChangeSet newChangeSet() {
        return new PrestoDefaultChangeSet(this, null);
    }

    @Override
    public PrestoChangeSet newChangeSet(ChangeSetHandler handler) {
        return new PrestoDefaultChangeSet(this, handler);
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

    public PrestoFieldResolver createFieldResolver(PrestoSchemaProvider schemaProvider, ObjectNode config) {
        PrestoContext context = new PrestoContext(schemaProvider, this, getObjectMapper());
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

    protected final int compareComparables(String o1, String o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }

}
