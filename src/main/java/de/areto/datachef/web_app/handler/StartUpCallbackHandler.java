package de.areto.datachef.web_app.handler;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.Application;
import de.areto.datachef.config.Constants;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.RepositoryConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.exceptions.DataChefException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpox;
import de.areto.datachef.jdbc.DbSpoxBuilder;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import lombok.extern.slf4j.Slf4j;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;

import static de.areto.datachef.config.Constants.DB_OK;
import static de.areto.datachef.persistence.HibernateUtility.CUSTOM_PROPERTIES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;

@WebRoute(path = "/startup", requestType = HttpMethod.post, template = "startup.vm")
@Slf4j
public class StartUpCallbackHandler extends TemplateRouteHandler {

    @Override
    public Map<String, Object> createContext(Request request, Response response) {
        Map<String, Object> context = new HashMap<>();
        final String action = request.queryParams("startupAction");

        if (!(action == null)) {
            switch (action) {

            case "Setup DWH":
                log.info("Setup DWH...");

                try {
                    final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
                    log.info("Reset Data Warehouse ~ {}", dwhConfig.jdbcConnectionString());
                    DWHSpox.setupDataWarehouse();
                } catch (DataChefException e) {
                    log.error("Error during reset of Data Warehouse: {}", e.getClass().getSimpleName());
                    if (e.getMessage() != null)
                        log.error("{}", e.getMessage());
                    context.put("errorMessageDWH", Constants.STARTUP_ERR_DWH_SETUP);
                }
                break;

            case "Setup Repo":
                log.info("Setup Repo...");

                CUSTOM_PROPERTIES.put("hibernate.hbm2ddl.auto", "create");

                try (Session session = HibernateUtility.getSessionFactory().openSession()) {
                    log.info("Try Repository ~ {}", session.getSession().isConnected() ? "OK" : "");
                } catch (ExceptionInInitializerError e) {
                    log.error("Error during try of Repository: {}", e.getClass().getSimpleName());
                    if (e.getMessage() != null)
                        log.error("{}", e.getMessage());
                    context.put("errorMessageRepo", Constants.STARTUP_ERR_REPO_UNHEALTHY);
                }

                final DbSpox spox = new DbSpoxBuilder().useConfig(ConfigCache.getOrCreate(RepositoryConfig.class))
                        .build();
                final String template = ConfigCache.getOrCreate(TemplateConfig.class).initializeRepositoryTemplate();
                final SQLTemplateRenderer renderer = new SQLTemplateRenderer(template);

                try {
                    final String sqlScript = renderer.render(Collections.emptyMap());
                    spox.executeScript(sqlScript);
                    log.info("Initial Repository data loaded");
                } catch (IOException | SQLException | RenderingException e) {
                    log.error("Error during reset of Repository: {}", e.getClass().getSimpleName());
                    if (e.getMessage() != null)
                        log.error("{}", e.getMessage());
                    context.put("errorMessageRepo", Constants.STARTUP_ERR_REPO_SETUP);
                }
                break;

            default:
                break;

            }

        }

        context.putAll(Application.get().checkDatabases());

        if (context.isEmpty()) {
            String targetPath = "/";
            request.session().attribute(DB_OK, true);
            Application.get().startServiceManager();
            if (request.session().attribute("startupRedirect") != null) {
                targetPath = request.session().attribute("startupRedirect");
            }
            response.redirect(targetPath);
            return Collections.emptyMap();
        } else {
            return context;
        }
    }

}
