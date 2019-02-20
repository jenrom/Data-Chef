package de.areto.datachef.service;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import de.areto.datachef.jdbc.DWHSpox;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SinkServiceTest {

    private static WorkerService workerService = new WorkerService();
    private static SinkService sinkService = new SinkService(workerService);

    public static void main(String[] args) throws Exception {
        DWHSpox.setupDataWarehouse();

        final Set<Service> services = new HashSet<>();
        services.add(workerService);
        services.add(sinkService);

        final ServiceManager manager = new ServiceManager(services);
        manager.addListener(new ServiceManager.Listener() {
            @Override
            public void healthy() {
                System.out.println("Services healthy");
            }

            @Override
            public void stopped() {
                System.out.println("Services stopped");
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
    }

}
