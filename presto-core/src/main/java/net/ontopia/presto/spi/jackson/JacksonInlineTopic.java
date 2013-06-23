package net.ontopia.presto.spi.jackson;

import org.codehaus.jackson.node.ObjectNode;

public class JacksonInlineTopic extends JacksonTopic {

    public JacksonInlineTopic(JacksonDataProvider dataProvider, ObjectNode data) {
        super(dataProvider, data);
    }

    @Override
    public boolean isInline() {
        return true;
    }

    @Override
    protected JacksonDataStrategy getDataStrategy() {
        return dataProvider.getInlineDataStrategy();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JacksonTopic) {
            JacksonTopic other = (JacksonTopic)o;
            if (!other.isInline()) {
                return false;
            } else {
                return getData().equals(other.getData());
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "InlineTopic[" + getId() + " " + getName() + "]";
    }

}
