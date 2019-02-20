package de.areto.datachef.parser.mapping;

import de.areto.datachef.jdbc.DWHSpox;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.MappingColumn;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

class MappingColumnValidator {


    private final Map<String, String> objectComments = new HashMap<>();
    private final Map<String, String> objectAliases = new HashMap<>();

    private final Mapping mapping;

    MappingColumnValidator(Mapping mapping) {
        this.mapping = mapping;
    }

    private void checkColumnName(String columnName) {
        try {
            if (DWHSpox.get().getReservedWords().contains(StringUtils.lowerCase(columnName))) {
                String msg = "Column '%s' - name is reserved by either Data Chef or the Database";
                msg = String.format(msg, columnName);
                mapping.addIssue(msg);
            }
        } catch (SQLException e) {
            String msg = "Column '%s' - Exception (%s) while checking reserved word status: %s";
            msg = String.format(msg, columnName, e.getClass().getSimpleName(), e.getMessage());
            mapping.addIssue(msg);
        }

        if (columnName.startsWith("chef_")) {
            String msg = "Column '%s' - name must not start with 'chef_'";
            msg = String.format(msg, columnName);
            mapping.addIssue(msg);
        }
    }

    Mapping validate() {
        for (MappingColumn column : mapping.getMappingColumns()) {
            checkColumnName(column.getName());
            if (column.hasNewName()) checkColumnName(column.getNewName());

            if (column.isKeyColumn() && column.isPartOfLinkSatellite()) {
                String msg = "Column '%s' - Key columns ('kc') cannot be part of a Links Satellite ('ls')";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (!column.hasDataDomainName()) {
                String msg = "Column '%s' - annotation is 'dd' required";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (!column.hasObjectName() && !column.isPartOfLinkSatellite()) {
                String msg = "Column '%s' - annotation 'on' is required";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.hasObjectComment() && !column.hasObjectName()) {
                String msg = "Column '%s' - annotation 'on' is required, because 'ocmnt' is provided";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.hasObjectComment() && objectComments.containsKey(column.getObjectComment())) {
                String msg = "Column '%s' - annotation 'ocmnt' is only allowed once per 'on'";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            } else {
                objectComments.put(column.getObjectName(), column.getObjectComment());
            }

            if (!column.hasObjectName() && column.hasObjectAlias()) {
                String msg = "Column '%s' - 'oa' is only permitted if 'on' is present";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.isIgnored() && column.isKeyColumn()) {
                String msg = "Column '%s' - key columns cannot be ignored";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.isIgnored() && column.isPartOfLinkSatellite()) {
                String msg = "Column '%s' - Link Satellite columns cannot be ignored";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.isIgnored() && column.hasSatelliteName()) {
                String msg = "Column '%s' - Ignored columns are not allowed define a Satellite ('sn')";
                msg = String.format(msg, column.getName());
                mapping.addIssue(msg);
            }

            if (column.hasObjectAlias()) {
                final boolean aliasStored = objectAliases.containsKey(column.getObjectName());
                if (aliasStored && !objectAliases.get(column.getObjectName()).equals(column.getObjectAlias())) {
                    String msg = "Column '%s' - Aliases ('oa') for the same object ('on') must be equal";
                    msg = String.format(msg, column.getName());
                    mapping.addIssue(msg);
                } else {
                    objectAliases.put(column.getObjectName(), column.getObjectAlias());
                }
            }
        }

        return mapping;
    }
}