package de.areto.common.util;

import spark.ResponseTransformer;

public class StringTransformer implements ResponseTransformer {
    @Override
    public String render(Object model) {
        if(model == null)
            return null;

        if(!(model instanceof String)) {
            return model.toString();
        }

        return (String) model;
    }
}
