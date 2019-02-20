package de.areto.datachef.persistence;

import de.areto.datachef.config.RepositoryConfig;
import lombok.experimental.UtilityClass;
import org.aeonbits.owner.ConfigCache;
import org.apache.commons.beanutils.PropertyUtils;
import org.hibernate.Hibernate;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

@UtilityClass
public class HibernateUtility {

    // ToDo (later): Hack - property modification should be done more elegantly in the future
    public static final Properties CUSTOM_PROPERTIES = new Properties();

    private static SessionFactory SESSION_FACTORY = null;

    private static SessionFactory buildSessionFactory() {
        try {
            // Start with: hibernate.cfg.xml
            final Configuration configuration = new Configuration();
            configuration.configure();
            configureFromFile();
            configuration.addProperties(CUSTOM_PROPERTIES);
            final SessionFactory sessionFactory = configuration.buildSessionFactory();
            Runtime.getRuntime().addShutdownHook(new Thread(sessionFactory::close));
            return sessionFactory;
        } catch (Throwable ex) {
            System.err.println("Initial SessionFactory creation failed." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    private static void configureFromFile() {
        final RepositoryConfig repoConfig = ConfigCache.getOrCreate(RepositoryConfig.class);

        if (repoConfig.jdbcConnectionString() != null)
            CUSTOM_PROPERTIES.setProperty("hibernate.connection.url", repoConfig.jdbcConnectionString());

        if (repoConfig.username() != null)
            CUSTOM_PROPERTIES.setProperty("hibernate.connection.username", repoConfig.username());

        if (repoConfig.password() != null)
            CUSTOM_PROPERTIES.setProperty("hibernate.connection.password", repoConfig.password());

        if (repoConfig.databaseDialect() != null)
            CUSTOM_PROPERTIES.setProperty("hibernate.dialect", repoConfig.databaseDialect());

        if (repoConfig.driverClass() != null)
            CUSTOM_PROPERTIES.setProperty("hibernate.connection.driver_class", repoConfig.driverClass());
    }

    public static SessionFactory getSessionFactory() {
        if (SESSION_FACTORY == null) SESSION_FACTORY = buildSessionFactory();
        return SESSION_FACTORY;
    }

    public static void shutdown() {
        getSessionFactory().close(); // Close caches and connection pools
    }

    public static <T> T initialize(T obj) throws Exception {
        Set<Object> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());
        try {
            initialize(obj, dejaVu);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new Exception("Error", e);
        }
        return obj;
    }

    private static void initialize(Object obj, Set<Object> dejaVu) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        if (dejaVu.contains(obj)) {
            return;
        } else {
            dejaVu.add(obj);

            if (!Hibernate.isInitialized(obj)) {
                Hibernate.initialize(obj);
            }
            PropertyDescriptor[] properties = PropertyUtils.getPropertyDescriptors(obj);
            for (PropertyDescriptor propertyDescriptor : properties) {
                Object origProp = PropertyUtils.getProperty(obj, propertyDescriptor.getName());
                if (origProp == null) return;

                if (origProp instanceof Collection) {
                    for (Object item : (Collection<?>) origProp) {
                        initialize(item, dejaVu);
                    }
                } else {
                    initialize(origProp, dejaVu);
                }
            }
        }
    }
}