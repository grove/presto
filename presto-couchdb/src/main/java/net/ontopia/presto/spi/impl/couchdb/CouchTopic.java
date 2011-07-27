package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class CouchTopic implements PrestoTopic {

	private final CouchDataProvider dataProvider;  
	private final ObjectNode data;

	protected CouchTopic(CouchDataProvider dataProvider, ObjectNode data) {
		this.dataProvider = dataProvider;
		this.data = data;    
	}

	protected CouchDataProvider getDataProvider() {
		return dataProvider;
	}

	public boolean equals(Object o) {
		if (o instanceof CouchTopic) {
			CouchTopic other = (CouchTopic)o;
			return other.getId().equals(getId());
		}
		return false;
	}

	public int hashCode() {
		return getId().hashCode();
	}

	protected static ObjectNode newInstanceObjectNode(CouchDataProvider dataProvider, PrestoType type) {
		ObjectNode data = dataProvider.getObjectMapper().createObjectNode();
		data.put(":type", type.getId());
		return data;
	}

	ObjectNode getData() {
		return data;
	}

	public String getId() {
		return data.get("_id").getTextValue();
	}

	public String getName() {
		JsonNode name = data.get(":name");
		return name == null ? null : name.getTextValue();
	}

	public String getTypeId() {
		return data.get(":type").getTextValue();
	}

	// json data access strategy

	protected abstract ArrayNode getFieldValue(PrestoField field);

	protected abstract void putFieldValue(PrestoField field, ArrayNode value);

	// methods for retrieving the state of a couchdb document

	public List<Object> getValues(PrestoField field) {
		return getValues(field, false, 0, -1).getValues();
	}

	public PagedValues getValues(PrestoField field, int offset, int limit) {
		return getValues(field, true, offset, limit);
	}

	protected PagedValues getValues(PrestoField field, boolean paging, int offset, int limit) {
		// get field values from data provider
		if (field.getId().startsWith("external:")) {
			return dataProvider.getExternalValues(this, field, paging, offset, limit);
		}

		// get field values from topic data
		List<Object> values = new ArrayList<Object>();
		ArrayNode fieldNode = getFieldValue(field);

		int size = fieldNode == null ? 0 : fieldNode.size();
		int start = 0;
		int end = size;
		if (paging) {
	    	final int DEFAULT_LIMIT = 40;
			int _limit = limit > 0 ? limit : DEFAULT_LIMIT;
			start = Math.min(Math.max(0, offset), size);
			end = Math.min(_limit+start, size);
		}
		
		if (fieldNode != null) { 
			if (field.isReferenceField()) {
				List<String> topicIds = new ArrayList<String>(fieldNode.size());
				for (int i=start; i < end; i ++) {
					JsonNode value = fieldNode.get(i);
					if (value.isTextual()) {
						topicIds.add(value.getTextValue());
					}
				}
				values.addAll(dataProvider.getTopicsByIds(topicIds));
			} else {
				for (int i=start; i < end; i ++) {
					JsonNode value = fieldNode.get(i);
					if (value.isTextual()) {
						values.add(value.getTextValue());
					} else {
						values.add(value.toString());
					}
				}
			}
		}
		return new CouchPagedValues(values, start, limit, size);
	}

	// methods for updating the state of a couchdb document

	private String getValue(Object value) {
		if (value instanceof CouchTopic) {
			CouchTopic valueTopic = (CouchTopic)value;
			return valueTopic.getId();
		} else {
			return(String)value;
		}
	}

	void setValue(PrestoField field, Collection<? extends Object> values) {
		ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
		for (Object value : values) {
			arrayNode.add(getValue(value));
		}
		putFieldValue(field, arrayNode);
	}

	void addValue(PrestoField field, Collection<? extends Object> values, int index) {
		if (!values.isEmpty()) {

			// remove duplicates (new)
			Set<String> addableValueIds = new HashSet<String>(values.size()); 
			for (Object value : values) {
				addableValueIds.add(getValue(value));
			}

			// remove duplicates (existing)
			Collection<String> existingValueIds = new LinkedHashSet<String>();
			ArrayNode jsonNode = getFieldValue(field);
			if (jsonNode != null) {
				for (JsonNode existing : jsonNode) {
					if (existing.isTextual()) {
						existingValueIds.add(existing.getTextValue());
					}
				}
			}

			List<String> result = new ArrayList<String>(existingValueIds.size() + addableValueIds.size());
			for (String valueId : existingValueIds) {
				result.add(valueId);
			}

			// remove duplicate values and decrement calculated index
			int calculatedIndex = index >= 0 && index < existingValueIds.size() ? index : existingValueIds.size(); 
			for (String valueId : addableValueIds) {
				int valueIndex = result.indexOf(valueId);
				if (valueIndex >= 0) {
					if (valueIndex < calculatedIndex) {
						calculatedIndex--;
					} 
					result.remove(valueIndex);
				}
			}

			// insert new values at calculated index
			for (String valueId : addableValueIds) {
				result.add(calculatedIndex, valueId);
			}

			// create new array node
			ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
			for (String value : result) {
				arrayNode.add(value);
			}
			putFieldValue(field, arrayNode);
		}
	}

	void removeValue(PrestoField field, Collection<? extends Object> values) {
		if (!values.isEmpty()) {
			ArrayNode jsonNode = getFieldValue(field);
			if (jsonNode != null) {
				Collection<String> existing = new LinkedHashSet<String>(jsonNode.size());
				for (JsonNode item : jsonNode) {
					existing.add(item.getValueAsText());
				}
				for (Object value : values) {
					existing.remove(getValue(value));
				}
				ArrayNode arrayNode  = dataProvider.getObjectMapper().createArrayNode();
				for (String value : existing) {
					arrayNode.add(value);
				}
				putFieldValue(field, arrayNode);
			}
		}
	}

	void updateNameProperty(Collection<? extends Object> values) {
		String name;
		Object value = values.isEmpty() ? null : values.iterator().next();
		if (value == null) {
			name = "No name";
		} else if (value instanceof CouchTopic) {
			name = ((CouchTopic)value).getName();
		} else {
			name = value.toString();
		}
		getData().put(":name", name);
	}

}
