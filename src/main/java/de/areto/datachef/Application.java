package de.areto.datachef;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import de.areto.datachef.config.Constants;
import de.areto.datachef.config.RepositoryConfig;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpoxBuilder;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.service.SinkService;
import de.areto.datachef.service.WorkerService;
import de.areto.datachef.web_app.RestService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Application {

    public static void main(String[] args) {
        Application.get();
    }

    private static Application instance;

    public static Application get() {
        if(instance == null) instance = new Application().start();
        return instance;
    }

    @Getter
    private final WorkerService workerService = new WorkerService();

    @Getter
    private final SinkService sinkService = new SinkService(workerService);

    @Getter
    private final SnowflakeSinkService snowflakeSinkService = new SnowflakeSinkService(workerService);
    
    private Application() { /* Singleton */ }

    private Application start() {
        log.info("Data Chef is starting...");
        RestService rs = new RestService();
        rs.startUp();
        return this;
    }
    volatile boolean isServiceManagerStarted = false;
    public synchronized void startServiceManager() {
        log.debug("starting ServiceManager... (isServiceManagerStarted="+isServiceManagerStarted+")");
        if (!isServiceManagerStarted) {
            final Set<Service> services = new HashSet<>();
            services.add(Application.get().getWorkerService());
            services.add(Application.get().getSinkService());
            services.add(Application.get().getSnowflakeSinkService());
            final ServiceManager manager = new ServiceManager(services);
            manager.addListener(new ServiceManager.Listener() {
                @Override
                public void healthy() {
                    log.info("DataChef is ready to serve...");
                    isServiceManagerStarted = true;
                }
                @Override
                public void stopped() {
                    log.info("DataChef has stopped");
                    isServiceManagerStarted = false;
                }
                @Override
                public void failure(Service service) {
                    log.error("Service '{}' failed, Cause: {}", service.getClass().getSimpleName(),
                            service.failureCause().getMessage());
                    isServiceManagerStarted = false;
                }
            });
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    manager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
                } catch (TimeoutException timeout) {
                    // stopping timed out
                }
            }));
            manager.startAsync();
        } else {
            log.debug("ServiceManager already started");
        }
    }
    public Map<String, Object> checkDatabases() {
        Map<String, Object> context = new HashMap<>();

        try {
            if(!DWHSpox.get().isHealthy()) {
                log.error("Data Warehouse is not reachable");
                context.put("errorMessageDWH", Constants.STARTUP_ERR_DWH_UNREACHABLE);
            }
        } catch (Exception e) {
            log.error("Data Warehouse is not healthy: ({}) {}", e.getClass().getSimpleName(), e.getMessage());
            context.put("errorMessageDWH", Constants.STARTUP_ERR_DWH_UNHEALTHY);
        }

        boolean repositoryReachable = new DbSpoxBuilder().useConfig(ConfigCache.getOrCreate(RepositoryConfig.class))
                .build().isReachable();
        if (!repositoryReachable) {
            log.error("Repository is not reachable");
            context.put("errorMessageRepo", Constants.STARTUP_ERR_REPO_UNREACHABLE);
        } else {
            try (Session session = HibernateUtility.getSessionFactory().openSession()) {
                log.info("Try Repository ~ {}", session.getSession().isConnected() ? "OK" : "");
            } catch (ExceptionInInitializerError e) {
                log.error("Error during try of Repository: {}", e.getClass().getSimpleName());
                if (e.getMessage() != null)
                    log.error("{}", e.getMessage());
                context.put("errorMessageRepo", Constants.STARTUP_ERR_REPO_UNHEALTHY);
            }
        }

        return context;
    }
}