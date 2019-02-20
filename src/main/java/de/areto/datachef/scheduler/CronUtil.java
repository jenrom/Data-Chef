package de.areto.datachef.scheduler;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.Locale;

@UtilityClass
public class CronUtil {

    private static final CronDefinition QUARTZ_DEF = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);

    public static String describeCronExpression(@NonNull String cronExpression) {
        final Cron cron = new CronParser(QUARTZ_DEF).parse(cronExpression).validate();
        final CronDescriptor descriptor = CronDescriptor.instance(Locale.ENGLISH);
        return descriptor.describe(cron);
    }

    public static CronParser getParser() {
        return new CronParser(QUARTZ_DEF);
    }

}
