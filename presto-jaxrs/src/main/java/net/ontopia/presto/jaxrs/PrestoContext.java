package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PrestoContext {

    private final PrestoTopic topic;
    private final PrestoType type;
    private final PrestoView view;

    private final boolean isNewTopic;
    
    private PrestoContext(Presto session, String topicId, String viewId) {
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        if (topicId.startsWith("_")) {
            type = schemaProvider.getTypeById(topicId.substring(1));
            topic = null;
            isNewTopic = true;
        } else {
            topic = dataProvider.getTopicById(topicId);
            type = topic == null ? null : schemaProvider.getTypeById(topic.getTypeId());
            isNewTopic = false;
        }
        if (type != null) {
            if (viewId == null) {
                view = type.getDefaultView();
            } else {
                view = type.getViewById(viewId);
            }
        } else {
            view = null;
        }
    }
    
    private PrestoContext (Presto session, PrestoType type, PrestoView view) {
        this(session, null, type, view, true);
    }

    private PrestoContext (Presto session, PrestoTopic topic, PrestoType type, PrestoView view) {
        this(session, topic, type, view, false);
    }
    
    private PrestoContext (Presto session, PrestoTopic topic, PrestoType type, PrestoView view, boolean isNewTopic) {
        this.topic = topic;
        this.type = type;
        this.view = view;
        this.isNewTopic = isNewTopic;
    }

    public static PrestoContext create(Presto session, String topicId) {
        return new PrestoContext(session, topicId, null);
    }
    
    public static PrestoContext create(Presto session, String topicId, String viewId) {
        return new PrestoContext(session, topicId, viewId);
    }

    public static PrestoContext create(Presto session, PrestoType type) {
        return new PrestoContext(session, type, type.getDefaultView());
    }
    
    public static PrestoContext create(Presto session, PrestoType type, PrestoView view) {
        return new PrestoContext(session, type, view);
    }
    
    public static PrestoContext create(Presto session, PrestoTopic topic, PrestoType type, PrestoView view) {
        return new PrestoContext(session, topic, type, view);
    }
    
    public boolean isMissingTopic() {
        return topic == null && !isNewTopic;
    }
    
    public boolean isNewTopic() {
        return isNewTopic;
    }
    
    public PrestoTopic getTopic() {
        return topic;
    }
    
    public PrestoType getType() {
        return type;
    }
    
    public PrestoView getView() {
        return view;
    }

    public PrestoFieldUsage getFieldById(String fieldId) {
        return type.getFieldById(fieldId, view);
    }
    
}
