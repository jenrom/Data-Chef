package de.areto.datachef.web_app.handler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import de.areto.common.util.JsonTransformer;
import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.sink.SinkFileFactory;
import de.areto.datachef.model.sink.SinkFileName;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@WebRoute(
        path = "/deploy/search",
        requestType = HttpMethod.post,
        responseTransformer = JsonTransformer.class
)
@Slf4j
public class DeploySearchDataHandler extends RouteHandler {

    static final Cache<String, List<String>> PATH_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .recordStats()
            .removalListener((RemovalNotification<String, List<String>> notification) -> {
                final Logger log = LoggerFactory.getLogger(DeploySearchDataHandler.class);
                if(log.isDebugEnabled())
                    log.debug("Search paths for deployment for Session '{}' evicted", notification.getKey());
            })
            .build();

    private final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);

    @Override
    public Object doWork(Request request, Response response) throws Exception {
        final String query = request.queryParams("query");

        if (query == null || query.isEmpty()) {
            throw new WebException("Parameter 'query' is empty or null");
        }

        final Path path = Paths.get(query);

        final List<String> searchResults = new LinkedList<>();
        searchResults.addAll(findMappings(path));
        searchResults.addAll(findDataFiles(path));

        PATH_CACHE.put(request.session().id(), searchResults);

        return searchResults;
    }

    private List<String> findMappings(@NonNull Path path) throws IOException {
        Predicate<Path> hasMappingExtension = p -> p.toString().endsWith(sinkConfig.mappingFileExtension())
                                                || p.toString().endsWith(sinkConfig.martFileExtension());

        return Files.walk(path)
                .filter(Files::isRegularFile)
                .filter(hasMappingExtension)
                .map(p -> p.toAbsolutePath().toString())
                .sorted()
                .collect(Collectors.toList());
    }

    private List<String> findDataFiles(@NonNull Path path) throws IOException {
        final Set<String> suppDataExt = new HashSet<>(sinkConfig.dataFileExtensions());

        return Files.walk(path)
                .filter(Files::isRegularFile)
                .filter(p -> {
                    final String ext = com.google.common.io.Files.getFileExtension(p.getFileName().toString());
                    return suppDataExt.contains(ext);
                })
                .map(SinkFileFactory::createFromPath)
                .sorted(Comparator.comparing(SinkFileName::getPublishDate).thenComparing(SinkFileName::getMappingName).thenComparing(SinkFileName::getFileGroup))
                .map(SinkFileName::getPath)
                .sorted()
                .collect(Collectors.toList());
    }
}
