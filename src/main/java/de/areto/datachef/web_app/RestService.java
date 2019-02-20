package de.areto.datachef.web_app;

import de.areto.common.template.WebTemplateRenderer;
import de.areto.common.util.ReflectionUtility;
import de.areto.datachef.config.Constants;
import de.areto.datachef.config.WebAppConfig;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.web_app.common.RouteConfig;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.reflections.Reflections;
import spark.Route;
import spark.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RestService {

    private final WebAppConfig webAppConfig = ConfigCache.getOrCreate(WebAppConfig.class);
    private Service sparkService;

    public void startUp() {
        this.sparkService = Service.ignite();

        if(webAppConfig.useExternalStaticFileLocation()) {
            this.sparkService.externalStaticFileLocation("public/");
        } else {
            this.sparkService.staticFileLocation(webAppConfig.staticFileLocation());
        }

        this.sparkService.port(webAppConfig.port()).threadPool(webAppConfig.threads());
        // Ensure databases are reachable and setup, User is logged in
        sparkService.before("*", (request, response) -> {
            final boolean isDBOK = request.session().attribute(Constants.DB_OK) != null;
            final boolean isLoggedIn = request.session().attribute(Constants.USER_SESSION_ATTRIBUTE_NAME) != null;
            final String pathInfo = request.pathInfo();

            if (!isDBOK && !pathInfo.equals("/startup")) {
                request.session().attribute("startupRedirect", pathInfo);
                response.redirect("/startup");
            }
            if (isDBOK && !isLoggedIn && !pathInfo.equals("/login")) {
                request.session().attribute("loginRedirect", pathInfo);
                response.redirect("/login");
            }
        });

        // All data is GZIP encoded
        sparkService.after("*", (req, res) -> res.header("Content-Encoding", "gzip"));

        // Render WebExceptions
        sparkService.exception(WebException.class, (e, req, res) -> {
            try {
                final String eMsg = e.getMessage();
                log.error("{}", eMsg == null ? "Error" : eMsg);

                final Map<String, Object> context = new HashMap<>();
                context.put("errorType", e.getClass().getSimpleName());
                context.put("errorMessage", eMsg == null ? "Error" : eMsg);

                final String content = new WebTemplateRenderer("error.vm").render(context);
                res.body(content);

            } catch (RenderingException e1) {
                log.error("Error rendering error page...");
            }
        });

        // 404
        sparkService.notFound((req, res) -> {
            final Map<String, Object> context = Collections.singletonMap("path", req.pathInfo());
            return new WebTemplateRenderer("not_found.vm").render(context);
        });

        registerHandler();
        sparkService.awaitInitialization();
    }

    private void registerHandler() {
        final Reflections reflections = new Reflections("de.areto.datachef");
        final Set<Class<? extends RouteHandler>> handlers = reflections.getSubTypesOf(RouteHandler.class);
        for (Class<? extends RouteHandler> handlerClass : handlers) {
            if (Modifier.isAbstract(handlerClass.getModifiers()))
                continue;

            if (!handlerClass.isAnnotationPresent(WebRoute.class)) {
                final String msg = String.format("Annotation %s must be present on %s implementations",
                        WebRoute.class, RouteHandler.class);
                throw new IllegalStateException(msg);
            }

            if (!ReflectionUtility.hasNoArgsConstructor(handlerClass) && handlerClass.getConstructors().length != 1) {
                final String msg = String.format("Implementations of %s must have only one parameterless constructor",
                        RouteHandler.class);
                throw new IllegalStateException(msg);
            }

            final RouteConfig routeConfig = RouteConfig.fromAnnotation(handlerClass.getAnnotation(WebRoute.class));

            switch (routeConfig.getRequestType()) {
                case get:
                    sparkService.get(routeConfig.getPath(), routeFromHandlerClass(handlerClass));
                    break;
                case post:
                    sparkService.post(routeConfig.getPath(), routeFromHandlerClass(handlerClass));
                    break;
                case put:
                    sparkService.put(routeConfig.getPath(), routeFromHandlerClass(handlerClass));
                    break;
                default:
                    throw new IllegalStateException("HttpMethod '" + routeConfig.getRequestType() + "' not supported");
            }
        }
    }

    private Route routeFromHandlerClass(Class<? extends RouteHandler> handler) {
        return (request, response) -> {
            try {
                final RouteHandler instance = (RouteHandler) handler.getConstructors()[0].newInstance();
                return instance.render(request, response);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                log.error("Unable to create RouteHandler instance, reason: {}", e);
                throw new Exception(e);
            }
        };
    }

    public void shutDown() {
        sparkService.stop();
    }
}
