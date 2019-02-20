package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.model.web.DataTableCallback;
import de.areto.datachef.model.web.DataTablePayload;
import de.areto.datachef.model.web.WorkerDataFileListRow;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import org.hibernate.query.Query;
import spark.Request;
import spark.Response;

import java.util.HashMap;
import java.util.Map;

@WebRoute(
        path = "/worker/data/file/list/data",
        responseTransformer = JsonTransformer.class
)
public class WorkerDataFileListDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        final DataTableCallback callback = DataTableCallback.fromRequest(request);

        final Map<Integer, String> orderColumnMap = new HashMap<>();

        orderColumnMap.put(0, "executionStartDateTime");
        orderColumnMap.put(1, "name");
        orderColumnMap.put(2, "fileName");
        orderColumnMap.put(3, "fileGroup");
        orderColumnMap.put(4, "publishDate");
        orderColumnMap.put(5, "payloadSize");
        orderColumnMap.put(6, "runtime");
        orderColumnMap.put(7, "status");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final boolean search = !callback.getSearchParam().isEmpty();

            final String whereCondition = "where d.name like :search " +
                    "or d.fileName like :search " +
                    "or d.status like :search ";

            final String qCounts = "select count(*) from MappingDataFileWorkerCargo d " + (search ? whereCondition : "");

            final Query<Long> countQuery = session.createQuery(qCounts, Long.class);
            if(search) countQuery.setParameter("search", "%" + callback.getSearchParam() + "%");

            final long recordsTotal = countQuery.getSingleResult();

            // long dbId, String type, String fileName, String fileGroup, long size, LocalDateTime publishDate, String mappingName, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize

            final String rowQuery = "select new " + WorkerDataFileListRow.class.getName() + "( " +
                    "d.dbId, d.fileName, d.fileGroup, d.dataSize, d.publishDate, d.name, d.executionStartDateTime, d.status, d.runtime, d.payloadSize " +
                    ") from MappingDataFileWorkerCargo d " +
                    (search ? whereCondition : "" ) +
                    "order by d." + orderColumnMap.get(callback.getOrderIndex()) + " " + callback.getOrderDirParam();

            final Query<WorkerDataFileListRow> query = session.createQuery(rowQuery, WorkerDataFileListRow.class);

            if(search) query.setParameter("search", "%" + callback.getSearchParam() + "%");

            query.setFirstResult(callback.getPage() * callback.getLength());
            query.setMaxResults(callback.getLength());

            final DataTablePayload<WorkerDataFileListRow> payload = new DataTablePayload<>();
            payload.setDraw(callback.getDraw());
            payload.setData(query.getResultList());
            payload.setRecordsFiltered(recordsTotal);
            payload.setRecordsTotal(recordsTotal);

            return payload;
        }
    }
}