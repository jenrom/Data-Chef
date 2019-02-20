package de.areto.datachef.persistence;

import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.mapping.CsvType;
import org.aeonbits.owner.ConfigCache;
import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.Session;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistenceTest {

    @Test
    public void dataDomainsShouldBePresent() throws Exception {
        final Session session = HibernateUtility.getSessionFactory().openSession();
        final int size = session.createQuery("from DataDomain ").getResultList().size();
        assertTrue(size > 0);
        session.close();
    }

    @Test
    public void csvTypesShouldBePresent() throws Exception {
        final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);
        final Session session = HibernateUtility.getSessionFactory().openSession();
        final Optional<CsvType> defCsvType = session.byId(CsvType.class).loadOptional(sinkConfig.defaultCsvType());
        assertTrue(defCsvType.isPresent());
        session.close();
    }

    @Test
    public void userShouldBePresent() throws Exception {
        final Session session = HibernateUtility.getSessionFactory().openSession();
        final int size = session.createQuery("from User").getResultList().size();
        assertThat(size).isGreaterThan(0);
    }

    @Test
    public void createUserPasswordHash() throws Exception {
        final String pw = "admin";
        final String hash = DigestUtils.md5Hex(pw);
        assertNotNull(hash);
    }
}
