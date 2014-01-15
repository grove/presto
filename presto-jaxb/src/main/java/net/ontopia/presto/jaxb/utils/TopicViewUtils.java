package net.ontopia.presto.jaxb.utils;

import java.util.Collection;
import java.util.Set;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;

public class TopicViewUtils {

    public static void copyFieldValues(TopicView toTopicView, TopicView fromTopicView, Set<String> ignoreFieldIds) {
        for (FieldData fromFieldData : fromTopicView.getFields()) {
            String fieldId = fromFieldData.getId();
            if (!ignoreFieldIds.contains(fieldId)) {
                Collection<Value> values = fromFieldData.getValues();
                setFieldValues(toTopicView, fieldId, values);
            }
        }
    }

    public static void setFieldValues(TopicView toTopicView, String fieldId, Collection<Value> values) {
        FieldData toFieldData = findFieldData(toTopicView, fieldId);
        if (toFieldData != null) {
            toFieldData.setValues(values);
        }
    }

    public static FieldData findFieldData(TopicView topicView, String fieldId) {
        for (FieldData fd : topicView.getFields()) {
            if (fd.getId().equals(fieldId)) {
                return fd;
            }
        }
        return null;
    }

}
