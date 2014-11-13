package net.ontopia.presto.jaxrs;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
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
        String startTopicId = PathParser.deskull(traverse[0]);
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
        PrestoField currentField = currentType.getFieldById(startFieldId, currentView);
        
        PrestoContext currentContext;
        if (isNewTopic) {
            currentContext = PrestoContext.create(session.getResolver(), currentType, currentView);
        } else {
            currentContext = PrestoContext.create(session.getResolver(), currentTopic, currentType, currentView);
        }
//        System.out.println("T1: " + startTopicId + " V: " + startViewId + " F: " + startFieldId);
        
        // traverse children
        for (int i=1; i < steps; i++) {
            String topicId = PathParser.deskull(traverse[i*3]);
            String viewId = traverse[i*3+1];
            String fieldId = traverse[i*3+2];
            
            if (PrestoContext.isNewTopic(topicId)) {
                currentTopic = null;
                currentType = PrestoContext.getTypeOfNewTopic(topicId, schemaProvider);
                currentView = currentType.getViewById(viewId);
                currentContext = PrestoContext.createSubContext(currentContext, currentField, topicId, viewId);
            } else {
                // find topic amongst parent field values
                currentTopic = findInParentField(currentContext, currentField, topicId);
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
        if (path == null || path.equals("_")) {
            return PrestoContext.create(session.getResolver(), PathParser.deskull(topicId), viewId);
        }

        PrestoContextField contextField = getContextField(session, path);
        
        PrestoContext currentContext = contextField.getContext();
        PrestoField currentField = contextField.getField();
        
        if (PrestoContext.isNewTopic(topicId)) {
            return PrestoContext.createSubContext(currentContext, currentField, PathParser.deskull(topicId), viewId);
        } else {
            PrestoTopic resultTopic = findInParentField(currentContext, currentField, topicId);
            if (resultTopic == null) {
                return null;
            } else {
                String resultTypeId = resultTopic.getTypeId();
                PrestoType resultType = session.getSchemaProvider().getTypeById(resultTypeId);
                PrestoView resultView = resultType.getViewById(viewId);
                return PrestoContext.createSubContext(currentContext, currentField, resultTopic, resultType, resultView);
            }
        }
    }

    private static PrestoTopic findInParentField(PrestoContext currentContext, PrestoField currentField, String topicId) {
        if (currentField.isReferenceField()) {
            for (Object value : currentContext.resolveValues(currentField)) {
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
        String localPath = PathParser.skull(topicId) + PathParser.FIELD_PATH_SEPARATOR + viewId + PathParser.FIELD_PATH_SEPARATOR + fieldId;

        PrestoContext parentContext = context.getParentContext();
        if (parentContext != null) {
            PrestoField parentField = context.getParentField();
            String parentPath =  getInlineTopicPath(parentContext, parentField);
            return parentPath + PathParser.FIELD_PATH_SEPARATOR + localPath;
        } else {
            return localPath;
        }
    }

    // WARN: replacing all / characters with skull character to 
    // work around http://java.net/jira/browse/JAX_RS_SPEC-70
    static final String SKULL_CHARACTER = "\u2620";

    public static String skull(String u) {
        // NOTE: we're only patching the ids of topics
        return u.replaceAll("/", PathParser.SKULL_CHARACTER);
    }

    public static String deskull(String u) {
        // NOTE: we're only patching the ids of topics
        return u.replaceAll(PathParser.SKULL_CHARACTER, "/");
    }
    
    public static String replaceUriPattern(String template, Map<String, String> values) {
        StringBuilder b = new StringBuilder(template.length());
        String PATTERN = "\\{(\\w+)\\}";
        Matcher m = Pattern.compile(PATTERN).matcher(template);
        int i = 0;
        while(m.find()) {
            b.append(template, i, m.start());
            String group = m.group(1);
            String value = values.get(group);
            if (value != null) {
                b.append(value);
            } else {
                b.append(m.group());
            }
            i = m.end();
        }
        b.append(template, i, template.length());
        return b.toString();
    }

    public static void main(String[] args) throws Exception {
        String template = "http://localhost/{something}/{databaseId}/{topicId}/{topicId}/{typeId}/{viewId}/";
        Map<String,String> values = new HashMap<String,String>();
        values.put("databaseId", "pesto");
        values.put("topicId", "ab/c");
        values.put("typeId", "A");
        values.put("viewId", "B");
        String result = replaceUriPattern(template, values);
        System.out.println("R: " + result);
    }

}
