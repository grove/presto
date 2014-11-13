package net.ontopia.presto.spi.jackson;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class JacksonLazyTopic extends JacksonTopic {

    public JacksonLazyTopic(JacksonDataProvider dataProvider, ObjectNode data) {
        super(dataProvider, data);
    }
    
    @Override
    public boolean isLazy() {
        return true;
    }

    @Override
    public String toString() {
        return "LazyTopic[" + getId() + " " + getName() + "]";
    }

}
