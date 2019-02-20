package de.areto.datachef.worker;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataChefConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.template.JobLogEntry;
import de.areto.datachef.model.template.JobLogEntryBuilder;
import de.areto.datachef.model.worker.MappingDataFileWorkerCargo;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.aeonbits.owner.ConfigCache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.areto.datachef.config.Constants.DF_PUB_DATE;

@UtilityClass
public class MaintenanceStatementFactory {

    public static String createWorkerPreRunStatement() throws RenderingException {
        final Map<String, Object> context = new HashMap<>();
        context.put("dwhConfig", ConfigCache.getOrCreate(DWHConfig.class));
        context.put("dataChefConfig", ConfigCache.getOrCreate(DataChefConfig.class));
        context.put("dataVaultConfig", ConfigCache.getOrCreate(DataVaultConfig.class));

        return new SQLTemplateRenderer(ConfigCache.getOrCreate(TemplateConfig.class).workerPreRunTemplate()).render(context);
    }

    public static String createWorkerPostRunStatement() throws RenderingException {
        final Map<String, Object> context = new HashMap<>();
        context.put("dwhConfig", ConfigCache.getOrCreate(DWHConfig.class));
        context.put("dataChefConfig", ConfigCache.getOrCreate(DataChefConfig.class));
        context.put("dataVaultConfig", ConfigCache.getOrCreate(DataVaultConfig.class));

        return new SQLTemplateRenderer(ConfigCache.getOrCreate(TemplateConfig.class).workerPostRunTemplate()).render(context);
    }

    public static <C extends WorkerCargo> String createInsertStatement(C cargo) throws RenderingException {
        final String workerName = cargo.getClass().getSimpleName().replace("Cargo", "");

        final JobLogEntry entry = new JobLogEntryBuilder()
                .setJobType(workerName)
                .setName(cargo.getName())
                .setRecordSource(cargo.getName())
                .setPublishDate(DF_PUB_DATE.format(cargo.getExecutionStartDateTime()))
                .setDataSize(-1L)
                .setFileChecksum("NA")
                .setPayloadSize(cargo.getPayloadSize())
                .setLoadTime(DF_PUB_DATE.format(cargo.getExecutionStartDateTime()))
                .setLoadTimeEnd(DF_PUB_DATE.format(cargo.getExecutionEndDateTime()))
                .setStatus(cargo.getStatus().toString())
                .setLoadId(cargo.getDbId()) // Hibernate DB ID get's LOAD_ID
                .createJobLogEntry();

        if(cargo instanceof MappingDataFileWorkerCargo) {
            final MappingDataFileWorkerCargo dataCargo = (MappingDataFileWorkerCargo) cargo;
            entry.setMappingGroup(dataCargo.getFileGroup());
            entry.setRecordSource(dataCargo.getFileName());
            entry.setPublishDate(DF_PUB_DATE.format(dataCargo.getPublishDate()));
            entry.setDataSize(dataCargo.getDataSize());
            entry.setFileChecksum(dataCargo.getCheckSum());
        }
        return renderInsertStatement(Collections.singletonMap("entry", entry));
    }

    private static String renderInsertStatement(Map<String, Object> context) throws RenderingException {
        return new SQLTemplateRenderer("dwh/job_log_insert.vm").render(context);
    }

    public static String renderUpdateStatement(@NonNull List<Long> idList) throws RenderingException {
        final Map<String, Object> context = Collections.singletonMap("idList", idList);
        return new SQLTemplateRenderer("dwh/job_log_update.vm").render(context);
    }

}