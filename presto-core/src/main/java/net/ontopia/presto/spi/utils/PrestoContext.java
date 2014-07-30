package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
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
    private PrestoField parentField;
    
    private PrestoContext(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, String topicId, String viewId) {
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
    
    public static PrestoContext create(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, String topicId, String viewId) {
        return new PrestoContext(dataProvider, schemaProvider, topicId, viewId);
    }
    
    public static PrestoContext create(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoTopic topic) {
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
        PrestoContext parentContext = context.getParentContext();
        PrestoField parentField = context.getParentField();
        PrestoType type = context.getType();
        PrestoView view = context.getView();
        return PrestoContext.createSubContext(parentContext, parentField, topic, type, view);
    }
    
    public static PrestoContext newContext(PrestoContext context, PrestoView view) {
        PrestoContext parentContext = context.getParentContext();
        PrestoField parentField = context.getParentField();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        return PrestoContext.createSubContext(parentContext, parentField, topic, type, view);
    }
    
    // create subcontexts
    
    public static PrestoContext createSubContext(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext parentContext, PrestoField parentField, PrestoTopic topic) {
        // ISSUE: shouldn't the view be parentField.getValueView(type) instead of type.getDefaultView()
        PrestoContext context = create(dataProvider, schemaProvider, topic);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        PrestoContext context = new PrestoContext(type, view);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(PrestoContext parentContext, PrestoField parentField, PrestoTopic topic, PrestoType type, PrestoView view) {
        PrestoContext context = new PrestoContext(topic, type, view);
        context.setParentContext(parentContext, parentField);
        return context;
    }
    
    public static PrestoContext createSubContext(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext parentContext, PrestoField parentField, String topicId, String viewId) {
        PrestoContext context = new PrestoContext(dataProvider, schemaProvider, topicId, viewId);
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
    
    public static String getTypeId(String topicId) {
        if (topicId != null && topicId.startsWith(NEW_TOPICID_PREFIX)) {
            String typeId = topicId.substring(1);
            return typeId; 
        }
        return null;
    }
    
    public PrestoContext getParentContext() {
        return parentContext;
    }
    
    public PrestoField getParentField() {
        return parentField;
    }
    
    private void setParentContext(PrestoContext parentContext, PrestoField parentField) {
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

    public PrestoField getFieldById(String fieldId) {
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
        sb.append(topicId);
        sb.append("$");
        sb.append(view.getId());
        return sb.toString();
    }

}
