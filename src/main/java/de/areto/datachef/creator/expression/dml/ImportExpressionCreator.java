package de.areto.datachef.creator.expression.dml;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.SQLExpressionCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static de.areto.datachef.model.compilation.SQLExpression.QueryType;

public class ImportExpressionCreator implements SQLExpressionCreator {

    private final Mapping mapping;

    public ImportExpressionCreator(@NonNull Mapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public SQLExpression createExpression() throws CreatorException {
        try {
            if (mapping.getStagingMode().equals(StagingMode.FILE))
                return createImportFileExpression();
            else if (mapping.getStagingMode().equals(StagingMode.CONNECTION))
                return createImportConnectionExpression();
            else // INSERT
                return createImportInsertExpression();
        } catch (RenderingException e) {
            throw new CreatorException(e);
        }
    }

    private SQLExpression createImportFileExpression() throws RenderingException {
        final StagingMode stgMode = StagingMode.FILE;
        checkState(mapping.getStagingMode().equals(stgMode), "Staging mode must be %s", stgMode);
        final Map<String, Object> context = Collections.singletonMap("mapping", mapping);
        final String description = String.format("Import files of type '%s' for Mapping '%s'",
                mapping.getCsvType().getName(), mapping.getName());
        final String template = ConfigCache.getOrCreate(TemplateConfig.class).importFileTemplate();
        final String sql = new SQLTemplateRenderer(template).render(context);
        return new SQLExpression(QueryType.IMPORT_FILE, sql, description);
    }

    private SQLExpression createImportConnectionExpression() throws RenderingException {
        final StagingMode stgMode = StagingMode.CONNECTION;
        checkState(mapping.getStagingMode().equals(stgMode), "Staging mode must be %s", stgMode);
        checkNotNull(mapping.getCustomSqlCode(), "Custom SQL must be set");
        checkState(!mapping.getCustomSqlCode().isEmpty(), "Custom SQL must be set");
        checkNotNull(mapping.getConnectionName(), "Connection name must be set");
        checkNotNull(mapping.getConnectionType(), "Connection type must be set");

        final Map<String, Object> context = Collections.singletonMap("mapping", mapping);
        final String description = String.format("Import custom SQL from connection ('%s') '%s' for Mapping '%s'",
                mapping.getConnectionType(), mapping.getConnectionName(), mapping.getName());
        final String template = ConfigCache.getOrCreate(TemplateConfig.class).importConnectionTemplate();
        final String sql = new SQLTemplateRenderer(template).render(context);
        return new SQLExpression(QueryType.IMPORT_CONNECTION, sql, description);
    }

    private SQLExpression createImportInsertExpression() throws RenderingException {
        final StagingMode stgMode = StagingMode.INSERT;
        checkState(mapping.getStagingMode().equals(stgMode), "Staging mode must be %s", stgMode);
        checkNotNull(mapping.getCustomSqlCode(), "Custom SQL must be set");
        checkState(!mapping.getCustomSqlCode().isEmpty(), "Custom SQL must be set");

        final Map<String, Object> context = Collections.singletonMap("mapping", mapping);
        final String desc = String.format("Import custom SQL as INSERT AS SELECT for Mapping '%s'", mapping.getName());
        final String template = ConfigCache.getOrCreate(TemplateConfig.class).importInsertTemplate();
        final String sql = new SQLTemplateRenderer(template).render(context);
        return new SQLExpression(QueryType.IMPORT_INSERT, sql, desc);
    }
}
