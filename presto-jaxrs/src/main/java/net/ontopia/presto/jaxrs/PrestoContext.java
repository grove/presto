package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PrestoContext {

    private final String topicId;
    
    private final PrestoTopic topic;
    private final PrestoType type;
    private final PrestoView view;

    private final boolean isNewTopic;
    private final boolean isReadOnly;
    
    private PrestoContext parentContext;
    private PrestoFieldUsage parentField;
    
    private PrestoContext(Presto session, String topicId, String viewId, boolean readOnly) {
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        if (topicId == null) {
            throw new RuntimeException("topicId cannot be null");
        } else if (topicId.startsWith("_")) {
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
        this.topicId = topicId;
        this.isReadOnly = readOnly;
    }

    private PrestoContext(Presto session, PrestoType type, PrestoView view, boolean readOnly) {
        this(session, null, type, view, true, readOnly);
    }

    private PrestoContext(Presto session, PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnly) {
        this(session, topic, type, view, false, readOnly);
    }
    
    private PrestoContext(Presto session, PrestoTopic topic, PrestoType type, PrestoView view, boolean isNewTopic, boolean readOnly) {
        this.topic = topic;
        this.topicId = (topic == null ? null :topic.getId());
        this.type = type;
        this.view = view;
        this.isNewTopic = isNewTopic;
        this.isReadOnly = readOnly;
    }

    public static PrestoContext create(Presto session, String topicId, boolean readOnly) {
        return new PrestoContext(session, topicId, null, readOnly);
    }
    
    public static PrestoContext create(Presto session, String topicId, String viewId, boolean readOnly) {
        return new PrestoContext(session, topicId, viewId, readOnly);
    }
    
    public static PrestoContext create(Presto session, PrestoTopic topic, boolean readOnly) {
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);
        return new PrestoContext(session, topic, type, type.getDefaultView(), readOnly);
    }
    
    public static PrestoContext create(Presto session, PrestoType type, PrestoView view, boolean readOnly) {
        return new PrestoContext(session, type, view, readOnly);
    }
    
    public static PrestoContext create(Presto session, PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnly) {
        return new PrestoContext(session, topic, type, view, readOnly);
    }
    
    public static PrestoContext createSubContext(Presto session, PrestoContext parentContext, PrestoFieldUsage parentField, PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnly) {
        PrestoContext context = new PrestoContext(session, topic, type, view, readOnly);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(Presto session, PrestoContext parentContext, PrestoField parentField, String topicId, String viewId, boolean readOnly) {
        PrestoContext context = new PrestoContext(session, topicId, viewId, readOnly);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public PrestoContext getParentContext() {
        return parentContext;
    }
    
    public PrestoFieldUsage getParentField() {
        return parentField;
    }
    
    private void setParentContext(PrestoContext parentContext, PrestoFieldUsage parentField) {
        this.parentContext = parentContext;
        this.parentField = parentField;
    }

    public boolean isMissingTopic() {
        return topic == null && !isNewTopic;
    }
    
    public boolean isNewTopic() {
        return isNewTopic;
    }
    
    public boolean isReadOnly() {
        return isReadOnly;
    }
    
    public PrestoTopic getTopic() {
        return topic;
    }
    
    public String getTopicId() {
        return topicId;
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
