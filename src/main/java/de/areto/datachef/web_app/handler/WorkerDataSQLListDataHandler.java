package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.model.web.DataTableCallback;
import de.areto.datachef.model.web.DataTablePayload;
import de.areto.datachef.model.web.WorkerDataSQLListRow;
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
        path = "/worker/data/sql/list/data",
        responseTransformer = JsonTransformer.class
)
public class WorkerDataSQLListDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        final DataTableCallback callback = DataTableCallback.fromRequest(request);

        final Map<Integer, String> orderColumnMap = new HashMap<>();

        /*
            <th>Exec. Date</th>
            <th>Type</th>
            <th>Mapping</th>
            <th>Payload</th>
            <th>Runtime</th>
            <th>Status</th>
        */

        orderColumnMap.put(0, "executionStartDateTime");
        orderColumnMap.put(1, "stagingMode");
        orderColumnMap.put(2, "name");
        orderColumnMap.put(4, "payloadSize");
        orderColumnMap.put(5, "runtime");
        orderColumnMap.put(6, "status");

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final boolean search = !callback.getSearchParam().isEmpty();

            final String whereCondition = "where d.name like :search " +
                    "or d.stagingMode like :search " +
                    "or d.status like :search ";

            final String qCounts = "select count(*) from MappingDataSQLWorkerCargo d " + (search ? whereCondition : "");

            final Query<Long> countQuery = session.createQuery(qCounts, Long.class);
            if(search) countQuery.setParameter("search", "%" + callback.getSearchParam() + "%");

            final long recordsTotal = countQuery.getSingleResult();

            // long dbId, String mappingName, StagingMode stagingMode, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize

            final String rowQuery = "select new " + WorkerDataSQLListRow.class.getName() + "( " +
                    "d.dbId, d.name, d.stagingMode, d.executionStartDateTime, d.status, d.runtime, d.payloadSize " +
                    ") from MappingDataSQLWorkerCargo d " +
                    (search ? whereCondition : "" ) +
                    "order by d." + orderColumnMap.get(callback.getOrderIndex()) + " " + callback.getOrderDirParam();

            final Query<WorkerDataSQLListRow> query = session.createQuery(rowQuery, WorkerDataSQLListRow.class);

            if(search) query.setParameter("search", "%" + callback.getSearchParam() + "%");

            query.setFirstResult(callback.getPage() * callback.getLength());
            query.setMaxResults(callback.getLength());

            final DataTablePayload<WorkerDataSQLListRow> payload = new DataTablePayload<>();
            payload.setDraw(callback.getDraw());
            payload.setData(query.getResultList());
            payload.setRecordsFiltered(recordsTotal);
            payload.setRecordsTotal(recordsTotal);

            return payload;
        }
    }
}