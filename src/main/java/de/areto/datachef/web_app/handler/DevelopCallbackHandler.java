package de.areto.datachef.web_app.handler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import de.areto.common.util.JsonTransformer;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.parser.SinkScriptParser;
import de.areto.datachef.persistence.HibernateInitializer;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;

import java.util.concurrent.TimeUnit;

@WebRoute(
        path = "/develop/callback",
        requestType = HttpMethod.post,
        responseTransformer = JsonTransformer.class
)
@Slf4j
public class DevelopCallbackHandler extends RouteHandler {

    static final Cache<String, Mapping> MAPPING_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .recordStats()
            .removalListener((RemovalNotification<String, Mapping> notification) -> {
                final Logger log = LoggerFactory.getLogger(DevelopCallbackHandler.class);
                if(log.isDebugEnabled())
                    log.debug("Cached and simulated mapping for Session '{}' evicted", notification.getKey());
            })
            .build();

    @Data
    private static class Result {
        private final String id;
        private final boolean valid;
    }

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        if(log.isDebugEnabled()) log.debug("Cache Stats: {}", MAPPING_CACHE.stats());

        final String code = request.queryParams("code");

        if(code == null || code.isEmpty()) {
            throw new WebException("Parameter 'code' is empty or null");
        }

        request.session().attribute("code", code);

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final String sessionId = request.session().id();
            final SinkScriptParser parser = new SinkScriptParser(sessionId, session);
            final Mapping mapping = parser.parse(code);
            //HibernateUtility.initialize(mapping);
            HibernateInitializer.initialize(mapping);
            MAPPING_CACHE.put(sessionId, mapping);
            return new Result(sessionId, mapping.isValid());
        }
    }
}
