package net.ontopia.presto.spi.impl.pojo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoSchemaModel {

    private static Logger log = LoggerFactory.getLogger(PojoSchemaModel.class.getName());

    public static void main(String[] args) throws Exception {
        PojoSchemaProvider schemaProvider = parse("pojo-schema-example", "pojo-schema-example.json");
        System.out.println("SP: " + schemaProvider + " " + schemaProvider.getDatabaseId());
    }

    public static PojoSchemaProvider parse(String databaseId, String schemaFilename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream istream = null;
        try {
            File schemaFile = new File(System.getProperty("user.home") + File.separator + schemaFilename);
            if (schemaFile.exists()) {
                log.warn("Loading presto schema model from file in user's home directory: " + schemaFile.getAbsolutePath());
                istream = new FileInputStream(schemaFile);
            } else {
                istream = cl.getResourceAsStream(schemaFilename);
            }
            if (istream == null) {
                throw new RuntimeException("Cannot find schema file: " + schemaFilename);
            }
            Reader reader = new InputStreamReader(istream, "UTF-8");
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.readValue(reader, ObjectNode.class);
            return createSchemaProvider(databaseId, objectNode);
        } catch (Exception e) {
            throw new RuntimeException("Problems occured when loading '" + schemaFilename + "'", e);
        } finally {
            try {
                if (istream != null) istream.close();
            } catch (IOException e) {
            }
        }
    }

    private static PojoSchemaProvider createSchemaProvider(String databaseId, ObjectNode json) {
        PojoSchemaProvider schemaProvider = new PojoSchemaProvider();
        schemaProvider.setDatabaseId(databaseId);

        Map<String, ObjectNode> fieldsMap = createFieldsMap(json);
        Map<String, ObjectNode> viewsMap = createViewsMap(json);
        Map<String, ObjectNode> typesMap = createTypesMap(json);

        Map<String,PojoType> types = new HashMap<String,PojoType>();
        for (String typeId : typesMap.keySet()) {
            ObjectNode typeConfig = typesMap.get(typeId);

            PojoType type = getPojoType(typeId, types, schemaProvider);
            schemaProvider.addType(type);

            // extra
            if (typeConfig.has("extra")) {
                type.setExtra(typeConfig.get("extra"));
            }

            // name
            String name = typeConfig.get("name").getTextValue();
            type.setName(name);
            // readOnly
            boolean readOnlyType = false;
            if (typeConfig.has("readOnly")) {
                readOnlyType = typeConfig.get("readOnly").getBooleanValue();
            }
            type.setReadOnly(readOnlyType);
            // hidden
            if (typeConfig.has("hidden")) {
                type.setHidden(typeConfig.get("hidden").getBooleanValue());
            }
            // creatable
            if (typeConfig.has("creatable")) {
                type.setCreatable(typeConfig.get("creatable").getBooleanValue());
            }
            // removable
            if (typeConfig.has("removable")) {
                type.setRemovable(typeConfig.get("removable").getBooleanValue());
            }
            // removableCascadingDelete
            if (typeConfig.has("removableCascadingDelete")) {
                type.setRemovableCascadingDelete(typeConfig.get("removableCascadingDelete").getBooleanValue());
            }

            // extends
            if (typeConfig.has("extends")) {
                String superTypeId = typeConfig.get("extends").getTextValue();
                verifyDeclaredType(superTypeId, typesMap, "extends", type);
                PojoType superType = getPojoType(superTypeId, types, schemaProvider);
                //                type.setSuperType(superType);
                superType.addDirectSubType(type);
            }
            if (typeConfig.has("views")) {
                ArrayNode viewsNode = (ArrayNode)typeConfig.get("views");
                for (JsonNode viewNode : viewsNode) {
                    String viewId;
                    ObjectNode viewConfig;

                    // view
                    if (viewNode.isTextual()) {
                        viewId = viewNode.getTextValue();                        
                        viewConfig = viewsMap.get(viewId);
                    } else if (viewNode.isObject()) {
                        viewConfig = (ObjectNode)viewNode;
                        viewId = viewConfig.get("id").getTextValue();
                    } else {
                        throw new RuntimeException("Invalid view declaration or view reference: " + viewNode);
                    }
                   
                    PojoView view = new PojoView(viewId, schemaProvider);
                    type.addView(view);
                    // view name
                    String viewName = viewConfig.get("name").getTextValue();
                    view.setName(viewName);

                    // extra
                    if (viewConfig.has("extra")) {
                        view.setExtra(viewConfig.get("extra"));
                    }

                    // fields
                    ArrayNode fieldsArray = (ArrayNode)viewConfig.get("fields");
                    for (JsonNode fieldNode : fieldsArray) {
                        String fieldId;
                        ObjectNode fieldConfig;
                        if (fieldNode.isTextual()) {
                            fieldId = fieldNode.getTextValue();                        
                            fieldConfig = fieldsMap.get(fieldId);
                        } else if (fieldNode.isObject()) {
                            fieldConfig = (ObjectNode)fieldNode;
                            fieldId = fieldConfig.get("id").getTextValue();
                        } else {
                            throw new RuntimeException("Invalid field declaration or field reference: " + fieldNode);
                        }
                        if (fieldId == null) {
                            throw new RuntimeException("Field id missing on field object: " + fieldConfig);
                        }
                        if (fieldConfig == null) {
                            throw new RuntimeException("Field declaration missing for field with id '" + fieldId + "'");
                        }

                        PojoField field = new PojoField(fieldId, schemaProvider);
                        type.addField(field);
                        field.addDefinedInView(view);
                        
                        // actualId
                        if (fieldConfig.has("actualId")) {
                            field.setActualId(fieldConfig.get("actualId").getTextValue());
                        }

                        // name
                        String fieldName = fieldConfig.get("name").getTextValue();                        
                        field.setName(fieldName);
                        // isNameField
                        if (fieldConfig.has("nameField")) {
                            field.setNameField(fieldConfig.get("nameField").getBooleanValue());
                        }

                        // isPrimitiveField/isReferenceField

                        // dataType
                        if (fieldConfig.has("datatype")) {
                            String datatype = fieldConfig.get("datatype").getTextValue();
                            field.setDataType(datatype);
                        } else {
                            field.setDataType("http://www.w3.org/2001/XMLSchema#string");
                        }
                        // extra
                        if (fieldConfig.has("extra")) {
                            field.setExtra(fieldConfig.get("extra"));
                        }
                        // valueView (using current view for now)
                        if (fieldConfig.has("valueView")) {
                            String valueViewId = fieldConfig.get("valueView").getTextValue();
                            PojoView valueView = new PojoView(valueViewId, schemaProvider);
                            field.setValueView(valueView);
                        } else {
                            field.setValueView(type.getDefaultView());
                        } 

                        // minCardinality
                        if (fieldConfig.has("minCardinality")) {
                            field.setMinCardinality(fieldConfig.get("minCardinality").getIntValue());
                        }
                        // maxCardinality
                        if (fieldConfig.has("maxCardinality")) {
                            field.setMaxCardinality(fieldConfig.get("maxCardinality").getIntValue());
                        }
                        // validationType
                        if (fieldConfig.has("validationType")) {
                            String validationType = fieldConfig.get("validationType").getTextValue();
                            field.setValidationType(validationType);
                        }                        
                        // isEmbedded
                        if (fieldConfig.has("embedded")) {
                            field.setEmbedded(fieldConfig.get("embedded").getBooleanValue());
                        }
                        // isHidden
                        if (fieldConfig.has("hidden")) {
                            field.setHidden(fieldConfig.get("hidden").getBooleanValue());
                        }
                        // isTraversable
                        if (fieldConfig.has("traversable")) {
                            field.setTraversable(fieldConfig.get("traversable").getBooleanValue());
                        }
                        // isReadOnly            
                        if (fieldConfig.has("readOnly")) {
                            field.setReadOnly(fieldConfig.get("readOnly").getBooleanValue());
                        } else {
                            field.setReadOnly(readOnlyType);
                        }
                        // isSorted
                        if (fieldConfig.has("sorted")) {
                            field.setSorted(fieldConfig.get("sorted").getBooleanValue());
                        }
                        // isPageable
                        if (fieldConfig.has("pageable")) {
                            field.setPageable(fieldConfig.get("pageable").getBooleanValue());
                        }
                        // limit
                        if (fieldConfig.has("limit")) {
                            field.setLimit(fieldConfig.get("limit").getIntValue());
                        }
                        // isCascadingDelete
                        if (fieldConfig.has("cascadingDelete")) {
                            field.setCascadingDelete(fieldConfig.get("cascadingDelete").getBooleanValue());
                        }
                        // isAddable
                        if (fieldConfig.has("addable")) {
                            field.setAddable(fieldConfig.get("addable").getBooleanValue());
                        }
                        // isRemovable
                        if (fieldConfig.has("removable")) {
                            field.setRemovable(fieldConfig.get("removable").getBooleanValue());
                        }
                        // isCreatable
                        if (fieldConfig.has("creatable")) {
                            field.setCreatable(fieldConfig.get("creatable").getBooleanValue());
                        }
                        // interfaceControl
                        if (fieldConfig.has("interfaceControl")) {
                            String interfaceControl = fieldConfig.get("interfaceControl").getTextValue();
                            field.setInterfaceControl(interfaceControl);
                        }

                        if (field.isReferenceField()) {

                            // availableFieldValueTypes
                            Set<PrestoType> valueTypes = new HashSet<PrestoType>();
                            if (fieldConfig.has("valueTypes")) {
                                ArrayNode valueTypesArray = (ArrayNode)fieldConfig.get("valueTypes");
                                for (JsonNode valueTypeIdNode : valueTypesArray) {
                                    String valueTypeId = valueTypeIdNode.getTextValue();
                                    verifyDeclaredType(valueTypeId, typesMap, "valueTypes",type, field);
                                    PojoType valueType = getPojoType(valueTypeId, types, schemaProvider);
                                    valueTypes.add(valueType);
                                }
                            }
                            field.setAvailableFieldValueType(valueTypes);
                        }

                        // availableFieldCreateTypes
                        if (fieldConfig.has("createTypes")) {
                            Set<PrestoType> createTypes = new HashSet<PrestoType>();
                            ArrayNode createTypesArray = (ArrayNode)fieldConfig.get("createTypes");
                            for (JsonNode createTypeIdNode : createTypesArray) {
                                String createTypeId = createTypeIdNode.getTextValue();
                                verifyDeclaredType(createTypeId, typesMap, "createTypes",type, field);
                                PojoType createType = getPojoType(createTypeId, types, schemaProvider);
                                createTypes.add(createType);
                            }
                            field.setAvailableFieldCreateType(createTypes);
                        }
                        
                        // valueAssignmentType
                        if (fieldConfig.has("valuesAssignmentType")) {
                            String valuesAssignmentType = fieldConfig.get("valuesAssignmentType").getTextValue();
                            field.setValuesAssignmentType(valuesAssignmentType);
                            
                            // values
                            if (fieldConfig.has("values")) {
                                List<String> values = new ArrayList<String>();
                                for (JsonNode value : fieldConfig.get("values")) {
                                    values.add(value.getTextValue());
                                }
                                field.setValues(values);
                            }
                        }

                    }
                }
            }
            types.put(typeId, type);            
        }
        return schemaProvider;
    }

    private static Map<String, ObjectNode> createTypesMap(ObjectNode json) {
        Map<String,ObjectNode> typesMap = new HashMap<String,ObjectNode>();
        if (json.has("types")) {
            ObjectNode typesNode = (ObjectNode)json.get("types");
            Iterator<String> typeNames = typesNode.getFieldNames();
            while (typeNames.hasNext()) {
                String typeName = typeNames.next();
                ObjectNode typeConfig = (ObjectNode)typesNode.get(typeName);
                typesMap.put(typeName, typeConfig);
            }
        }
        return typesMap;
    }

    private static Map<String, ObjectNode> createFieldsMap(ObjectNode json) {
        Map<String,ObjectNode> fieldsMap = new HashMap<String,ObjectNode>();
        if (json.has("fields")) {
            ObjectNode fieldsNode = (ObjectNode)json.get("fields");
            Iterator<String> fieldNames = fieldsNode.getFieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                ObjectNode fieldConfig = (ObjectNode)fieldsNode.get(fieldName);
                fieldsMap.put(fieldName, fieldConfig);
            }
        }
        return fieldsMap;
    }

    private static Map<String, ObjectNode> createViewsMap(ObjectNode json) {
        Map<String,ObjectNode> viewsMap = new HashMap<String,ObjectNode>();
        if (json.has("views")) {
            ObjectNode viewsNode = (ObjectNode)json.get("views");
            Iterator<String> fieldNames = viewsNode.getFieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                ObjectNode viewConfig = (ObjectNode)viewsNode.get(fieldName);
                viewsMap.put(fieldName, viewConfig);
            }
        }
        return viewsMap;
    }

    private static void verifyDeclaredType(String typeId, Map<String, ObjectNode> typesMap, String jsonField, PojoType type) {
        if (!typesMap.containsKey(typeId)) {
            throw new RuntimeException("Unknown type '" + typeId + "' in " + jsonField + " on type '" + type.getId() + "'");
        }
    }

    private static void verifyDeclaredType(String typeId, Map<String, ObjectNode> typesMap, String jsonField, PojoType type, PojoField field) {
        if (!typesMap.containsKey(typeId)) {
            throw new RuntimeException("Unknown type '" + typeId + "' in " + jsonField + " in field '" + field.getId() + "' on type '" + type.getId() + "'");
        }
    }

    private static PojoType getPojoType(String typeId, Map<String,PojoType> types, PojoSchemaProvider schemaProvider) {
        PojoType type = types.get(typeId);
        if (type == null) {
            type = new PojoType(typeId, schemaProvider);
            types.put(typeId, type);
        }
        return type;
    }

}
