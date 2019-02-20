package de.areto.datachef.web_app.common;

import de.areto.common.util.StringTransformer;
import spark.ResponseTransformer;
import spark.route.HttpMethod;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WebRoute {
    String path();
    HttpMethod requestType() default HttpMethod.get;
    ContentType contentType() default ContentType.WILDCARD;
    Class<? extends ResponseTransformer> responseTransformer() default StringTransformer.class;

    String DEFAULT_TEMPLATE = "?";
    String template() default DEFAULT_TEMPLATE;

}
