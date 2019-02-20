package de.areto.datachef.web_app.common;

import lombok.Data;
import spark.ResponseTransformer;
import spark.route.HttpMethod;

@Data
public class RouteConfig {
    private final String path;
    private HttpMethod requestType;
    private ContentType contentType;
    private Class<? extends ResponseTransformer> responseTransformer;
    private String template;

    public static RouteConfig fromAnnotation(WebRoute annotation) {
        if (!annotation.path().startsWith("/")) {
            throw new IllegalArgumentException("Path has to start with a slash");
        }
        if(!annotation.template().equals(WebRoute.DEFAULT_TEMPLATE) && annotation.template().isEmpty()) {
            throw new IllegalArgumentException("Empty templates are not allowed");
        }

        final RouteConfig config = new RouteConfig(annotation.path());
        config.setContentType(annotation.contentType());
        config.setRequestType(annotation.requestType());
        config.setResponseTransformer(annotation.responseTransformer());
        config.setTemplate(annotation.template());
        return config;
    }
}
