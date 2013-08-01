package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PrestoContext {

    private static final String NEW_TOPICID_PREFIX = "_";

    private final String topicId;
    
    private final PrestoTopic topic;
    private final PrestoType type;
    private final PrestoView view;

    private final boolean isNewTopic;
    
    private PrestoContext parentContext;
    private PrestoFieldUsage parentField;
    
    private PrestoContext(Presto session, String topicId, String viewId) {
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        if (topicId == null) {
            throw new RuntimeException("topicId cannot be null");
        } else if (isNewTopic(topicId)) {
            type = getType(topicId, schemaProvider);
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
    }

    private PrestoContext(PrestoType type, PrestoView view) {
        this(null, type, view);
    }

    private PrestoContext(PrestoTopic topic, PrestoType type, PrestoView view) {
        this.topic = topic;
        this.topicId = (topic == null ? NEW_TOPICID_PREFIX + type.getId() : topic.getId());
        this.type = type;
        this.view = view;
        this.isNewTopic = (topic == null);
    }
    
    // create new contexts
    
    public static PrestoContext create(Presto session, String topicId, String viewId) {
        return new PrestoContext(session, topicId, viewId);
    }
    
    public static PrestoContext create(Presto session, PrestoTopic topic) {
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);
        return new PrestoContext(topic, type, type.getDefaultView());
    }
    
    public static PrestoContext create(PrestoType type, PrestoView view) {
        return new PrestoContext(type, view);
    }
    
    public static PrestoContext create(PrestoTopic topic, PrestoType type, PrestoView view) {
        return new PrestoContext(topic, type, view);
    }
    
    public static PrestoContext newContext(PrestoContext context, PrestoTopic topic) {
        return PrestoContext.create(topic, context.getType(), context.getView());
    }
    
    // create subcontexts
    
    public static PrestoContext createSubContext(Presto session, PrestoContext parentContext, PrestoFieldUsage parentField, PrestoTopic topic) {
        PrestoContext context = create(session, topic);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(PrestoContext parentContext, PrestoFieldUsage parentField, PrestoTopic topic, PrestoType type, PrestoView view) {
        PrestoContext context = new PrestoContext(topic, type, view);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(Presto session, PrestoContext parentContext, PrestoFieldUsage parentField, String topicId, String viewId) {
        PrestoContext context = new PrestoContext(session, topicId, viewId);
        context.setParentContext(parentContext, parentField);
        return context;
    }

    // methods
    
    public static boolean isNewTopic(String topicId) {
        return topicId.startsWith(NEW_TOPICID_PREFIX);
    }
 
    public static PrestoType getType(String topicId, PrestoSchemaProvider schemaProvider) {
        String typeId = topicId.substring(1);
        return schemaProvider.getTypeById(typeId);
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
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (parentContext != null) {
            sb.append(parentContext.toString());
            sb.append("$");
        }
        if (parentField != null) {
            sb.append(parentField.getId());
            sb.append("$");
        }
        sb.append(topic.getId());
        sb.append("$");
        sb.append(view.getId());
        return sb.toString();
    }

}
