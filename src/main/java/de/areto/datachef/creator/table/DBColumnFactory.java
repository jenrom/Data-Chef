package de.areto.datachef.creator.table;

import de.areto.datachef.config.ConfigUtility;
import de.areto.datachef.config.Constants;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.jdbc.DbSpoxUtility;
import de.areto.datachef.model.datavault.*;
import de.areto.datachef.model.jdbc.DBColumn;
import de.areto.datachef.model.mart.MartColumn;
import de.areto.datachef.persistence.HibernateUtility;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.HibernateException;
import org.hibernate.Session;

import java.sql.SQLException;
import java.util.Optional;

@UtilityClass
public class DBColumnFactory {

    private static DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    public static DBColumn createKeyColumn(@NonNull DVObject object) throws CreatorException {
        final DVObject parent = object.isSatellite() ? ((Satellite) object).getParent() : object;
        final String colName = String.format("%s%s", parent.getName(), parent.getKeySuffix());
        final String comment = String.format("PK of %s '%s'", parent.getType(), parent.getName());
        final String keyDomainName = ConfigUtility.getKeyDomainName(object);
        final DataDomain dataDomain = getDataDomain(keyDomainName);
        return create(colName, comment, false, dataDomain);
    }

    public static DBColumn createLoadIdColumn() throws CreatorException {
        final DataDomain idDataDomain = getDataDomain(dvConfig.loadIdDomain());
        return create(dvConfig.loadIdName(), Constants.COL_CMNT_LOAD_ID, false, idDataDomain);
    }

    public static DBColumn createLastSeenColumn() throws CreatorException {
        final DataDomain dataDomain = getDataDomain(dvConfig.lastSeenDateDomain());
        return create(dvConfig.lastSeenDateName(), Constants.COL_CMNT_LAST_SEEN, false, dataDomain);
    }

    public static DBColumn createDiffHashColumn(@NonNull Satellite satellite) throws CreatorException {
        final DataDomain dataDomain = getDataDomain(dvConfig.satDiffKeyDataDomain());
        final String colName = satellite.getName() + dvConfig.satDiffKeySuffix();
        final String comment = "Diff. hash for Satellite " + satellite.getName();
        return create(colName, comment, false, dataDomain);
    }

    public static DBColumn createLoadDateColumn() throws CreatorException {
        final DataDomain dataDomain = getDataDomain(dvConfig.loadDateDomain());
        final String colName = dvConfig.loadDateName();
        return create(colName, Constants.COL_CMNT_LOAD_DATE_FROM, false, dataDomain);
    }

    public static DBColumn createLoadDateEndColumn() throws CreatorException {
        final DataDomain dataDomain = getDataDomain(dvConfig.loadDateEndDomain());
        final String colName = dvConfig.loadDateEndName();
        return create(colName, Constants.COL_CMNT_LOAD_DATE_END, false, dataDomain);
    }

    public static DBColumn createFromLeg(@NonNull Leg leg) throws CreatorException {
        final String comment = "FK to " + leg.getHub();
        final String colName = leg.getHub().getName() + dvConfig.hubKeySuffix();
        final DataDomain dataDomain = getDataDomain(dvConfig.hubKeyDataDomain());
        return create(colName, comment, false, dataDomain);
    }

    public static DBColumn createFromDVColumn(@NonNull DVColumn dvColumn) throws CreatorException {
        return create(dvColumn.getName(), null, true, dvColumn.getDataDomain());
    }

    public static DBColumn create(@NonNull String name, String cmnt, boolean nullable, @NonNull DataDomain dataDomain) throws CreatorException {
        final DBColumn col = new DBColumn();
        final String sqlTypeString = dataDomain.getSqlType();
        final String[] colTypeParts;
        try {
            colTypeParts = getSqlTypeParts(sqlTypeString);
        } catch (SQLException e) {
            final String msg = String.format("Unable get SQL type parts for type '%s', reason: %s", sqlTypeString, e.getMessage());
            throw new CreatorException(msg);
        }

        col.setName(name);
        if (cmnt != null) {
            col.setComment(DbSpoxUtility.escapeSingleQuotes(cmnt));
        }
        col.setNullable(nullable);

        col.setPrecision(colTypeParts[1] == null ? 0 : Integer.parseInt(colTypeParts[1]));
        col.setScale(colTypeParts[2] == null ? 0 : Integer.parseInt(colTypeParts[2]));
        col.setTypeName(colTypeParts[0]);
        return col;
    }

    public static DataDomain getDataDomain(@NonNull String domainName) throws CreatorException {
        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final Optional<DataDomain> dataDomain = session.byId(DataDomain.class).loadOptional(domainName);
            final String msg = String.format("DataDomain %s not found", domainName);
            return dataDomain.orElseThrow(() -> new CreatorException(msg));
        } catch (HibernateException e) {
            final String msg = String.format("Unable to get DataDomain %s, reason: %s", domainName, e.getMessage());
            throw new CreatorException(msg, e);
        }
    }

    public static String[] getSqlTypeParts(@NonNull String sqlType) throws SQLException {
        final String typeParts[] = DbSpoxUtility.parseSqlType(sqlType);

        if (typeParts.length == 0) {
            final String msg = "Unable to parse type parts from '" + sqlType + "'";
            throw new SQLException(msg);
        }

        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        final String dataTypeName = typeParts[0];

        if (dwhConfig.scanAndCheckSupportedDataTypes() && !DWHSpox.get().getTypeMap().containsKey(dataTypeName)) {
            final String msg = String.format("SQL type '%s' is not supported by database", dataTypeName);
            throw new SQLException(msg);
        }

        return typeParts;
    }

    public static DBColumn from(@NonNull MartColumn martColumn) throws CreatorException {
        final DBColumn dbColumn = DBColumnFactory.create(martColumn.getName(),
                martColumn.getComment(),
                true,
                martColumn.getDataDomain());
        dbColumn.setIdentity(martColumn.isIdentityColumn());
        return dbColumn;
    }
}
