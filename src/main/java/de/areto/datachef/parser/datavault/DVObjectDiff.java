package de.areto.datachef.parser.datavault;

import de.areto.datachef.model.datavault.*;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// ToDo (later): Refactor, Generics, Inheritance
public class DVObjectDiff {

    private final List<String> differences = new ArrayList<>();
    private final DVObject leftObject;
    private final DVObject rightObject;

    public DVObjectDiff(DVObject leftObject, DVObject rightObject) {
        this.leftObject = leftObject;
        this.rightObject = rightObject;
    }

    public boolean hasDifferences() {
        return !differences.isEmpty();
    }

    public List<String> getDifferences() {
        return differences;
    }

    public void analyze() {
        if (!leftObject.getType().equals(rightObject.getType())) {
            String msg = "Comparison not possible, different object types {0} and {1}";
            addDifference(msg, leftObject.getType(), rightObject.getType());
            return;
        }
        if (!leftObject.getName().equals(rightObject.getName())) {
            String msg = "Comparison not possible, different object names {0} and {1}";
            addDifference(msg, leftObject.getName(), rightObject.getName());
            return;
        }

        if (leftObject.getType().equals(DVObject.Type.HUB)) {
            analyzeHubs((Hub) leftObject, (Hub) rightObject);
        } else if (leftObject.getType().equals(DVObject.Type.LNK)) {
            analyzeLinks((Link) leftObject, (Link) rightObject);
        } else {
            analyzeSatellites((Satellite) leftObject, (Satellite) rightObject);
        }
        analyzeColumns();
    }

    private void analyzeSatellites(Satellite left, Satellite right) {
        if (!left.getParent().getType().equals(right.getParent().getType())) {
            String msg = "Mismatched Satellite parent types ''{0}'' != ''{1}''";
            addDifference(msg, left.getParent(), right.getParent());
        }
        if (!left.getParent().getName().equals(right.getParent().getName())) {
            String msg = "Mismatched Satellite parent names ''{0}'' != ''{1}''";
            addDifference(msg, left.getParent(), right.getParent());
        }
    }

    private void analyzeLinks(Link left, Link right) {
        if(left.getLegs().size() != right.getLegs().size()) {
            final String msg = "Links have different leg counts {0} vs. {1}";
            addDifference(msg, left.getLegs().size(), right.getLegs().size());
        }

        if(left.isSelfReference() != right.isSelfReference()) {
            final String msg = "Links have different self reference flags: {0} vs. {1}";
            addDifference(msg, left.isSelfReference(), right.isSelfReference());
        }

        if(left.isHistoricized() != right.isHistoricized()) {
            final String msg = "Links have different historicize flags: {0} vs. {1}";
            addDifference(msg, left.isHistoricized(), right.isHistoricized());
        }

        for(Leg leg : left.getLegs()) {
            if(!right.hasLeg(leg.getRole(), leg.getHub())) {
                final String msg = "Leg of ''{2}'' for Hub ''{0}'' in role ''{1}'' is missing";
                addDifference(msg, leg.getHub(), leg.getRole(), left);
            }
        }

        for(Leg leg : right.getLegs()) {
            if(!left.hasLeg(leg.getRole(), leg.getHub())) {
                final String msg = "Leg of ''{2}'' for Hub ''{0}'' in role ''{1}'' is missing";
                addDifference(msg, leg.getHub(), leg.getRole(), right);
            }
        }
    }

    private void analyzeHubs(Hub left, Hub right) {
        if (left.hasAlias() && !right.hasAlias()) {
            addDifference("Only left Hub ({0}) has an alias ''{1}''", left.getName(), left.getAlias());
        } else if (!left.hasAlias() && right.hasAlias()) {
            addDifference("Only right Hub ({0}) has an alias ''{1}''", right.getName(), right.getAlias());
        } else if (!Objects.equals(left.getAlias(), right.getAlias())) {
            String msg = "Hubs define different aliases ''{0}'' != ''{1}''";
            addDifference(msg, left.getAlias(), right.getAlias());
        }
    }

    private void analyzeColumns() {
        if (leftObject.getColumns().size() != rightObject.getColumns().size()) {
            String msg = "Different column counts ''{0}'' != ''{1}''";
            addDifference(msg, leftObject.getColumns().size(), rightObject.getColumns().size());
        }

        for (DVColumn leftColumn : leftObject.getColumns()) {
            if (rightObject.getColumnByName(leftColumn.getName()) == null) {
                String msg = "Right object must define column ''{0}''";
                addDifference(msg, leftColumn.getName());
                continue;
            }
            DVColumn rightColumn = rightObject.getColumnByName(leftColumn.getName());
            if (!leftColumn.getDataDomain().equals(rightColumn.getDataDomain())) {
                String msg = "Columns ''{0}'' have different Data Domains ''{1}'' != ''{2}''";
                addDifference(msg, leftColumn.getName(), leftColumn.getDataDomain(), rightColumn.getDataDomain());
            }

            if(leftColumn.isKeyColumn() != rightColumn.isKeyColumn()) {
                String msg = "Columns ''{0}'' key column ('kc') values don't match ''{1}'' != ''{2}''";
                addDifference(msg, leftColumn.getName(), leftColumn.isKeyColumn(), rightColumn.isKeyColumn());
            }
        }
    }

    private void addDifference(String msg, Object... args) {
        String compareString = "({0}) {1} vs. ({2}) {3}: ";
        compareString = MessageFormat.format(compareString, leftObject.getType(), leftObject.getName(), rightObject.getType(), rightObject.getName());
        String msg2 = MessageFormat.format(msg, args);
        differences.add(compareString + msg2);
    }
}