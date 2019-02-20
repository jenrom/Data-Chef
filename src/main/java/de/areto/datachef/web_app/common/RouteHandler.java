package de.areto.datachef.web_app.common;

import de.areto.datachef.exceptions.WebException;
import lombok.Getter;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;

public abstract class RouteHandler {

    @Getter
    private final RouteConfig routeConfig;

    public RouteHandler() {
        this.routeConfig = RouteConfig.fromAnnotation(getAnnotation());
    }

    private WebRoute getAnnotation() {
        if(!getClass().isAnnotationPresent(WebRoute.class)) {
            throw new IllegalStateException("Annotation " + WebRoute.class + " must be present");
        }
        return getClass().getAnnotation(WebRoute.class);
    }

    public abstract Object doWork(Request request, Response response) throws Exception;

    public Object render(Request request, Response response) throws WebException {
        try {
            final Object result = doWork(request, response);
            final ResponseTransformer transformer = routeConfig.getResponseTransformer().newInstance();
            return transformer.render(result);
        } catch (Throwable t) {
            throw new WebException(t);
        }
    }

}
