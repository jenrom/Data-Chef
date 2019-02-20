package de.areto.datachef.web_app.handler;

import de.areto.common.util.JsonTransformer;
import de.areto.datachef.model.web.DataTableCallback;
import de.areto.datachef.model.web.DataTablePayload;
import de.areto.datachef.model.web.RollbackListRow;
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
        path = "/rollbacks/list/data",
        responseTransformer = JsonTransformer.class
)
public class RollbacksListDataHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        final DataTableCallback callback = DataTableCallback.fromRequest(request);

        final Map<Integer, String> orderColumnMap = new HashMap<>();
        orderColumnMap.put(0, "executionStartDateTime");
        orderColumnMap.put(1, "dbId");
        orderColumnMap.put(2, "type");
        orderColumnMap.put(3, "name");
        orderColumnMap.put(4, "payloadSize");
        orderColumnMap.put(5, "runtime");
        orderColumnMap.put(6, "status");

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final boolean search = !callback.getSearchParam().isEmpty();
            final String whereCondition = "where m.name like :search ";

            final String qCounts = "select count(*) from RollbackWorkerCargo m " + (search ? whereCondition : "");

            final Query<Long> totalQuery = session.createQuery(qCounts, Long.class);
            if (search) totalQuery.setParameter("search", "%" + callback.getSearchParam() + "%");
            final long recordsTotal = totalQuery.getSingleResult();

            // long dbId, String type, String name, LocalDateTime executionDateTime, WorkerCargo.Status status, long runtime, long payloadSize

            final String qRows = "select new " + RollbackListRow.class.getName() + "(" +
                    " m.dbId, m.rollbackType, m.name, m.executionStartDateTime, m.status, m.runtime, m.payloadSize " +
                    ") from RollbackWorkerCargo m  " +
                    (search ? whereCondition : "") +
                    "order by m." + orderColumnMap.get(callback.getOrderIndex()) + " " + callback.getOrderDirParam();

            final Query<RollbackListRow> query = session.createQuery(qRows, RollbackListRow.class);
            if (search) query.setParameter("search", "%" + callback.getSearchParam() + "%");

            query.setFirstResult(callback.getPage() * callback.getLength());
            query.setMaxResults(callback.getLength());

            final DataTablePayload<RollbackListRow> payload = new DataTablePayload<>();
            payload.setDraw(callback.getDraw());
            payload.setData(query.getResultList());
            payload.setRecordsFiltered(recordsTotal);
            payload.setRecordsTotal(recordsTotal);

            return payload;
        }
    }
}
