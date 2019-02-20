package de.areto.datachef.jdbc;

import com.google.common.collect.Sets;
import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.DataChefException;
import de.areto.datachef.exceptions.DbSpoxException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.jdbc.DBColumnType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigCache;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Singleton class providing a cache for the JDBC meta data of the EXASOL based DWH. Meta data is retrieved
 * using {@link DbSpox} configured via {@link DWHConfig}.
 */
@Slf4j
public class DWHSpox {

    private static DWHSpox instance;

    private final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);

    private Set<String> schemaSet;
    private Map<String, DBColumnType> typeMap;
    private Set<String> reservedWords;

    public static DWHSpox get() {
        if (instance == null)
            instance = new DWHSpox();
        return instance;
    }

    public static void setupDataWarehouse() throws DataChefException {
        try {
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).setupDwhTemplate();
            final SQLTemplateRenderer renderer = new SQLTemplateRenderer(template);
            final String sqlScript = renderer.render(Collections.emptyMap());

            final DbSpox spox = new DbSpoxBuilder().useConfig(ConfigCache.getOrCreate(DWHConfig.class)).build();
            final int[] res = spox.executeScript(sqlScript);

            log.debug("Executed {} statements", res.length);
        } catch (IOException | SQLException | RenderingException e) {
            final String msg = "Unable to reset Data Warehouse";
            throw new DataChefException(msg, e);
        }
    }

    public boolean isHealthy() throws DbSpoxException, SQLException {
        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        final DbSpox spox = new DbSpoxBuilder().useConfig(dwhConfig).build();

        if (!spox.isReachable())
            return false;

        if(!dwhConfig.dbType().equals(DbType.EXASOL) && !spox.getCatalogs().contains(this.dwhConfig.catalog())) {
            throw new DbSpoxException(String.format("Catalog '%s' is missing", this.dwhConfig.catalog()));
        }

        final Set<String> dwhSchemas = Sets.newHashSet(
                dwhConfig.schemaNameServed(),
                dwhConfig.schemaNameCooked(),
                dwhConfig.schemaNameRaw(),
                dwhConfig.schemaNameAdmin(),
                dwhConfig.schemaNameViews()
        );

        final Set<String> availableSchemas = spox.getSchemas();

        for(String schema : dwhSchemas) {
            if(!availableSchemas.contains(schema))
                throw new DbSpoxException(String.format("Schema '%s' is missing", schema));
        }

        return true;
    }

    public Set<String> getSchemaSet() throws SQLException {
        if (schemaSet != null) {
            return schemaSet;
        }

        final DbSpox dbSpox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        schemaSet = dbSpox.getSchemas();
        return schemaSet;
    }

    public Map<String, DBColumnType> getTypeMap() throws SQLException {
        if (typeMap != null) {
            return typeMap;
        }

        final DbSpox dbSpox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        typeMap = dbSpox.getTypes();
        return typeMap;
    }

    public Set<String> getReservedWords() throws SQLException {
        if (reservedWords != null)
            return reservedWords;

        final DbSpox dbSpox = new DbSpoxBuilder().useConfig(dwhConfig).build();
        reservedWords = dbSpox.getReservedWords();
        return reservedWords;
    }

    public String getSchema(@NonNull String schemaName) throws SQLException {
        final Set<String> schemaSet = getSchemaSet();
        if (schemaSet.contains(schemaName)) {
            return schemaName;
        } else {
            final String msg = String.format("Schema '%s' does not exist", schemaName);
            throw new SQLException(msg);
        }
    }

}
