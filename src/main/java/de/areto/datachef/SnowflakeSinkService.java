package de.areto.datachef;

import com.google.common.util.concurrent.AbstractIdleService;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.jdbc.DbSpox;
import de.areto.datachef.jdbc.DbSpoxBuilder;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.service.WorkerService;
import de.areto.datachef.worker.MappingDataFileWorker;
import de.areto.datachef.worker.SinkDataFile;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class SnowflakeSinkService extends AbstractIdleService {
    private WorkerService workerService;

    public SnowflakeSinkService(WorkerService workerService) {

        this.workerService = workerService;
    }

    @Override
    protected void startUp() throws Exception {
        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        DbSpox dbSpox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        Runnable task = () -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }
            log.info("Staring snowflake sink service");
            String fileToLoad = "datachef_stage/api_order_items.20190320_111935103.csv.gz";
            SinkDataFile dataFile = new SinkDataFile("N/A",
                    "api_order_items.20190320_111935103.csv.gz",
                    "93e54d5e77841ba0b16568e920947eec",
                    4177488l,
                    "api_order_items",
                    LocalDateTime.of(2019,4,1,1,1),
                    fileToLoad
            );
            MappingDataFileWorker fileWorker = new MappingDataFileWorker(dataFile);
            workerService.execute(fileWorker);


        };
        executorService.execute(task);

    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Shutting down snowflake sink service");
    }
}
