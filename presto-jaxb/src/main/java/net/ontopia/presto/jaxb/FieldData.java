package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class FieldData extends Document {
    
    private String id;
    private String name;
    
    private Boolean readOnly;
    private Boolean creatable;
    private Boolean editable;

    private Boolean embeddable;
    private Boolean pageable;

    private String datatype;
    private String validation;
    private String interfaceControl;
    
    private Integer valuesLimit;
    private Integer valuesOffset;
    private Integer valuesTotal;
    
    private Map<String,Object> params;

    private Integer minCardinality;
    private Integer maxCardinality;

    private Collection<Link> links;
    private Collection<Value> values;
    
    private Collection<Value> availableValues;
    
    private Collection<FieldData> valueFields;

    private Collection<TopicType> valueTypes;
    private Collection<TopicType> createTypes;

    private Collection<Message> messages;
    private Collection<String> errors;

    public static class Message {
        private String type;
        private String message;

        public Message() {
        }
        
        public Message(String type, String message) {
            this.type = type;
            this.message = message;
            
        }
        public String getType() {
            return type;
        }
        public void setType(String type) {
            this.type = type;
        }
        public String getMessage() {
            return message;
        }
        public void setMessage(String message) {
            this.message = message;
        }
    }

    public FieldData() {
    }
    
    @Override
    public String getFormat() {
        return "field";
    }
    
    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setLinks(Collection<Link> links) {
        if (links.isEmpty()) {
            this.links = null;
        } else {
            this.links = links;
        }
    }

    public Collection<Link> getLinks() {
        return links;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public void setCreatable(Boolean creatable) {
        this.creatable = creatable;
    }

    public Boolean isCreatable() {
        return creatable;
    }

    public void setEditable(Boolean editable) {
        this.editable = editable;
    }

    public Boolean isEditable() {
        return editable;
    }

    public Boolean isEmbeddable() {
        return embeddable;
    }

    public void setEmbeddable(Boolean embeddable) {
        this.embeddable = embeddable;
    }

    public Boolean isPageable() {
        return pageable;
    }

    public void setPageable(Boolean pageable) {
        this.pageable = pageable;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setValidation(String validation) {
        this.validation = validation;
    }

    public String getValidation() {
        return validation;
    }

    public void setInterfaceControl(String interfaceControl) {
        this.interfaceControl = interfaceControl;
    }

    public String getInterfaceControl() {
        return interfaceControl;
    }

    public void setMinCardinality(Integer minCardinality) {
        this.minCardinality = minCardinality;
    }

    public Integer getMinCardinality() {
        return minCardinality;
    }

    public void setMaxCardinality(Integer maxCardinality) {
        this.maxCardinality = maxCardinality;
    }

    public Integer getMaxCardinality() {
        return maxCardinality;
    }

    public void setValues(Collection<Value> values) {
        this.values = values;
    }

    public Collection<Value> getValues() {
        return values;
    }
    
    public Collection<TopicType> getValueTypes() {
        return valueTypes;
    }

    public void setValueTypes(Collection<TopicType> valueTypes) {
        this.valueTypes = valueTypes;
    }
    
    public Collection<TopicType> getCreateTypes() {
        return createTypes;
    }

    public void setCreateTypes(Collection<TopicType> createTypes) {
        this.createTypes = createTypes;
    }

    public Collection<FieldData> getValueFields() {
        return valueFields;
    }

    public void setValueFields(Collection<FieldData> fields) {
        this.valueFields = fields;
    }

    public Integer getValuesLimit() {
        return valuesLimit;
    }

    public void setValuesLimit(Integer valuesLimit) {
        this.valuesLimit = valuesLimit;
    }

    public Integer getValuesOffset() {
        return valuesOffset;
    }

    public void setValuesOffset(Integer valuesOffset) {
        this.valuesOffset = valuesOffset;
    }

    public Integer getValuesTotal() {
        return valuesTotal;
    }

    public void setValuesTotal(Integer valuesTotal) {
        this.valuesTotal = valuesTotal;
    }

    public Collection<String> getErrors() {
        return errors;
    }

    public void setErrors(Collection<String> errors) {
        this.errors = errors;
    }

    public Collection<Message> getMessages() {
        return messages;
    }

    public void setMessages(Collection<Message> messages) {
        this.messages = messages;
    }

    public Map<String,Object> getParams() {
        return params;
    }

    public void setParams(Map<String,Object> params) {
        this.params = params;
    }

    public Collection<Value> getAvailableValues() {
        return availableValues;
    }

    public void setAvailableValues(Collection<Value> availableValues) {
        this.availableValues = availableValues;
    }
}
