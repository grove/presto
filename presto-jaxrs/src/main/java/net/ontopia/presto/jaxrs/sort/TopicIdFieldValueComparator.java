package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.Utils;

public class TopicIdFieldValueComparator extends FieldValueComparator {

    @Override
    public int compare(Object o1, Object o2) {
        String t1 = getTopicId(o1);
        String t2 = getTopicId(o2);
        return Utils.compareComparables(t1, t2);
    }

    private String getTopicId(Object o) {
        if (o instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)o;
            return topic.getId();
        }
        return null;
    }

}
