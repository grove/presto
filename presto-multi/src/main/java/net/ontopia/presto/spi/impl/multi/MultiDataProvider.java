package net.ontopia.presto.spi.impl.multi;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MultiDataProvider implements PrestoDataProvider {

    private static Logger log = LoggerFactory.getLogger(MultiDataProvider.class.getName());

    private final Map<String,PrestoDataProvider> dataProviders = new HashMap<String,PrestoDataProvider>();

    public MultiDataProvider() {
    }
    
    @Override
    public String getProviderId() {
        return "multi";
    }

    protected PrestoDataProvider getDataProvider(String providerId) {
        PrestoDataProvider dataProvider = dataProviders.get(providerId);
        if (dataProvider == null) {
            dataProvider = createDataProvider(providerId);
            dataProviders.put(providerId, dataProvider);
        }
        return dataProvider;
    }
    
    protected abstract PrestoDataProvider createDataProvider(String providerId);
    
    @Override
    public PrestoTopic getTopicById(String id) {
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        for (PrestoDataProvider dataProvider : dataProviders.values()) {
            try {
                dataProvider.close();
            } catch (Exception e) {
                log.warn("Problems while closing data provider.", e);
            }
        }
    }

}
