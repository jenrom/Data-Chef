package de.areto.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.ResponseTransformer;

public class JsonTransformer implements ResponseTransformer {
    @Override
    public String render(Object model) throws Exception {
        if(model == null) return null;
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        return objectMapper.writeValueAsString(model);
    }
}
