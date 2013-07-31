package net.ontopia.presto.jaxrs.process;

import java.util.ArrayList;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;

public class SubmittedState {

    private final TopicView topicView;
    
    public SubmittedState(TopicView topicView) {
        this.topicView = topicView;
    }

    public List<? extends Object> getValues(String fieldId) {
        List<Object> result = null;
        for (FieldData fd : topicView.getFields()) {
            if (fieldId.equals(fd.getId())) {
                for (Value v : fd.getValues()) {
                    String value = v.getValue();
                    if (value != null) {
                        if (result == null) {
                            result = new ArrayList<Object>();
                        }
                        result.add(value);
                    }
                }
            }
        }
        return result;
    }
    
}
