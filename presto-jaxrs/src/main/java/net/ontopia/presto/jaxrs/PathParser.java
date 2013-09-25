package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextField;

public class PathParser {
    
    private static final String FIELD_PATH_SEPARATOR = "$";
    
    private static final String FIELD_PATH_SEPARATOR_REGEX = "\\" + FIELD_PATH_SEPARATOR;
    
    public static PrestoContextField getContextField(Presto session, String path) {
        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

        String[] traverse = path.split(FIELD_PATH_SEPARATOR_REGEX);
        if (traverse.length % 3 != 0) {
            throw new RuntimeException("Invalid field path: " + path);
        }
        int steps = traverse.length / 3;

        // find root topic
        String startTopicId = Links.deskull(traverse[0]);
        String startViewId = traverse[1];
        String startFieldId = traverse[2];

        boolean isNewTopic = PrestoContext.isNewTopic(startTopicId); 

        PrestoTopic currentTopic;
        String startTypeId;
        if (isNewTopic) {
            currentTopic = null;
            startTypeId = PrestoContext.getTypeId(startTopicId);
        } else {
            currentTopic = dataProvider.getTopicById(startTopicId);
            if (currentTopic == null) {
                return null;
            }
            startTypeId = currentTopic.getTypeId();
        }
        
        PrestoType currentType = schemaProvider.getTypeById(startTypeId);
        PrestoView currentView = currentType.getViewById(startViewId);
        PrestoFieldUsage currentField = currentType.getFieldById(startFieldId, currentView);
        
        PrestoContext currentContext;
        if (isNewTopic) {
            currentContext = PrestoContext.create(currentType, currentView);
        } else {
            currentContext = PrestoContext.create(currentTopic, currentType, currentView);
        }
//        System.out.println("T1: " + startTopicId + " V: " + startViewId + " F: " + startFieldId);
        
        // traverse children
        for (int i=1; i < steps; i++) {
            String topicId = Links.deskull(traverse[i*3]);
            String viewId = traverse[i*3+1];
            String fieldId = traverse[i*3+2];
            
            if (PrestoContext.isNewTopic(topicId)) {
                currentTopic = null;
                currentType = currentContext.getType();
                currentView = currentContext.getView();
                currentContext = PrestoContext.createSubContext(dataProvider, schemaProvider, currentContext, currentField, topicId, viewId);
            } else {
                // find topic amongst parent field values
                currentTopic = findInParentField(currentTopic, currentField, topicId);
                if (currentTopic == null) {
                    return null;
                }
                String typeId = currentTopic.getTypeId();
                currentType = schemaProvider.getTypeById(typeId);
                currentView = currentType.getViewById(viewId);
                currentContext = PrestoContext.createSubContext(currentContext, currentField, currentTopic, currentType, currentView);
            }
            currentField = currentType.getFieldById(fieldId, currentView);
//            System.out.println("T0: " + topicId + " V: " + viewId + " F: " + fieldId);
        }
        return new PrestoContextField(currentContext, currentField);
    }
    
    public static PrestoContext getTopicByPath(Presto session, String path, String topicId, String viewId) {
        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

        if (path == null || path.equals("_")) {
            return PrestoContext.create(dataProvider, schemaProvider, Links.deskull(topicId), viewId);
        }

        PrestoContextField contextField = getContextField(session, path);
        
        PrestoContext currentContext = contextField.getContext();
        PrestoTopic currentTopic = currentContext.getTopic();
        PrestoFieldUsage currentField = contextField.getField();
        
        PrestoTopic resultTopic = findInParentField(currentTopic, currentField, topicId);
        if (resultTopic == null) {
            return PrestoContext.createSubContext(dataProvider, schemaProvider, currentContext, currentField, Links.deskull(topicId), viewId);
        } else {
            String resultTypeId = resultTopic.getTypeId();
            PrestoType resultType = session.getSchemaProvider().getTypeById(resultTypeId);
            PrestoView resultView = resultType.getViewById(viewId);
            return PrestoContext.createSubContext(currentContext, currentField, resultTopic, resultType, resultView);
        }
    }

    private static PrestoTopic findInParentField(PrestoTopic currentTopic, PrestoField currentField, String topicId) {
        if (currentField.isReferenceField()) {
            for (Object value : currentTopic.getValues(currentField)) {
                PrestoTopic valueTopic = (PrestoTopic)value;
                if (topicId.equals(valueTopic.getId())) {
                    return valueTopic;
                }
            }
        }
        return null;
    }
    
    public static String getInlineTopicPath(PrestoContext context, PrestoField field) {
        if (context == null) {
            return "_";
        }
        String topicId = context.getTopicId();
        String viewId = context.getView().getId();
        String fieldId = field.getId();
        String localPath = Links.skull(topicId) + PathParser.FIELD_PATH_SEPARATOR + viewId + PathParser.FIELD_PATH_SEPARATOR + fieldId;

        PrestoContext parentContext = context.getParentContext();
        if (parentContext != null) {
            PrestoField parentField = context.getParentField();
            String parentPath =  getInlineTopicPath(parentContext, parentField);
            return parentPath + PathParser.FIELD_PATH_SEPARATOR + localPath;
        } else {
            return localPath;
        }
    }

}
