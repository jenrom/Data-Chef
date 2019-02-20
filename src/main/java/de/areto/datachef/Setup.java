package de.areto.datachef;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.RepositoryConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.DataChefException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpox;
import de.areto.datachef.jdbc.DbSpoxBuilder;
import de.areto.datachef.persistence.HibernateUtility;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

import static de.areto.datachef.persistence.HibernateUtility.CUSTOM_PROPERTIES;

public class Setup {

    private static final Logger LOG = LoggerFactory.getLogger(Setup.class);

    public static void main(String[] args) {
        setup();
        System.exit(0);
    }

    public static void setup() {
        LOG.info("!!!! DataChef: Setup !!!!");
        setupDataWarehouse();
        resetRepository();
        LOG.info("Done.");
    }

    private static void resetRepository() {
        CUSTOM_PROPERTIES.put("hibernate.hbm2ddl.auto", "create");

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            LOG.info("Reset Repository ~ {}", session.getSession().isConnected() ? "OK" : "");
        } catch (Exception e) {
            LOG.error("Error during reset of Repository: {}", e.getClass().getSimpleName());
            if (e.getMessage() != null) LOG.error("{}", e.getMessage());
            System.exit(-1);
        }

        final DbSpox spox = new DbSpoxBuilder().useConfig(ConfigCache.getOrCreate(RepositoryConfig.class)).build();
        final String template = ConfigCache.getOrCreate(TemplateConfig.class).initializeRepositoryTemplate();
        final SQLTemplateRenderer renderer = new SQLTemplateRenderer(template);

        try {
            final String sqlScript = renderer.render(Collections.emptyMap());
            spox.executeScript(sqlScript);
            LOG.info("Initial Repository data loaded");
        } catch ( IOException | SQLException | RenderingException e) {
            LOG.error("Error during reset of Repository: {}", e.getClass().getSimpleName());
            if (e.getMessage() != null) LOG.error("{}", e.getMessage());
            System.exit(-1);
        }
    }

    private static void setupDataWarehouse() {
        try {
            final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
            LOG.info("Reset Data Warehouse ~ {}", dwhConfig.jdbcConnectionString());
            DWHSpox.setupDataWarehouse();
        } catch (DataChefException e) {
            LOG.error("Error during reset of Data Warehouse: {}", e.getClass().getSimpleName());
            if (e.getMessage() != null) LOG.error("{}", e.getMessage());
            System.exit(-1);
        }
    }
}
