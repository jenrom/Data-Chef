package de.areto.test;

import com.google.common.base.Preconditions;
import de.areto.datachef.comparators.DVObjectLoadComparator;
import de.areto.datachef.model.datavault.DVObject;
import de.areto.datachef.model.datavault.Hub;
import de.areto.datachef.model.datavault.Satellite;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.sink.SinkFileFactory;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.datachef.parser.SinkScriptParser;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.worker.Worker;
import lombok.experimental.UtilityClass;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.proxy.HibernateProxy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@UtilityClass
public class TestUtility {

    public static <T extends DVObject> T getObject(Class<T> clazz, Session session, String name) {
        final DVObject.Type type;

        if (clazz.equals(Hub.class))
            type = DVObject.Type.HUB;
        else if (clazz.equals(Satellite.class))
            type = DVObject.Type.SAT;
        else
            type = DVObject.Type.LNK;

        final Optional<DVObject> object = session.byNaturalId(DVObject.class)
                .using(DVObject.TYPE_COLUMN, type)
                .using(DVObject.IDENTIFIER_COLUMN, name)
                .loadOptional();

        assertTrue(object.isPresent());

        if (Hibernate.getClass(object.get()).equals(clazz)) {
            @SuppressWarnings("unchecked") final T instance = (T) initAndUnproxy(object.get());
            return instance;
        } else {
            throw new IllegalStateException("Wrong class");
        }
    }

    public static Mapping parseAndPublish(SinkFile sinkFile) throws Exception {
        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final Transaction transaction = session.beginTransaction();

            final String mappingName = sinkFile.getMappingName();

            final SinkScriptParser scriptParser = new SinkScriptParser(mappingName, session);
            final Mapping mapping = scriptParser.parse(sinkFile.getContentString());

            mapping.getIssueList().forEach(System.err::println);

            if (mapping.isValid()) {
                final Collection<DVObject> mappedObjects = mapping.getMappedObjects();
                mappedObjects.stream().sorted(new DVObjectLoadComparator()).forEach(session::persist);
                session.persist(mapping);
                transaction.commit();
            } else {
                transaction.rollback();
            }

            return mapping;
        }
    }

    public static SinkFile getSinkFileFromResource(String res) throws URISyntaxException, IOException {
        final URL resUrl = TestUtility.class.getResource(res);
        final Path resPath = Paths.get(resUrl.toURI());

        SinkFile.Type type = SinkFile.Type.DATA;
        if(resPath.endsWith(".sink")) type = SinkFile.Type.MAPPING;
        if(resPath.endsWith(".mart")) type = SinkFile.Type.MART;

        return SinkFileFactory.create(resPath, type);
    }

    public static WorkerCargo executeWorker(Worker worker) {
        try {
            final WorkerCargo cargo = worker.call();
            cargo.getErrors().forEach(System.err::println);
            return cargo;
        } catch (Exception e) {
            fail("Worker execution resulted in exception: " + e);
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T initAndUnproxy(T entity) {
        Preconditions.checkNotNull(entity, "Entity is null");

        Hibernate.initialize(entity);

        if (entity instanceof HibernateProxy) {
            entity = (T) ((HibernateProxy) entity).getHibernateLazyInitializer()
                    .getImplementation();
        }
        return entity;
    }
}
