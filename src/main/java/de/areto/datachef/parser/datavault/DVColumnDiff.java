package de.areto.datachef.parser.datavault;

import de.areto.datachef.model.datavault.DVColumn;
import lombok.Getter;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class DVColumnDiff {

    final DVColumn leftColumn;
    final DVColumn rightColumn;

    @Getter
    private final List<String> differences = new ArrayList<>();

    public DVColumnDiff(DVColumn leftColumn, DVColumn rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    public boolean analyze() {
        if(!leftColumn.getName().equals(rightColumn.getName())) {
            addDifference("Names are not equal");
            return false;
        }

        if (!leftColumn.getDataDomain().equals(rightColumn.getDataDomain())) {
            String msg = "Different Data Domains ''{0}'' != ''{1}''";
            addDifference(msg, leftColumn.getDataDomain(), rightColumn.getDataDomain());
            return false;
        }

        if(leftColumn.isKeyColumn() != rightColumn.isKeyColumn()) {
            String msg = "Key column ('kc') values don't match ''{1}'' != ''{2}''";
            addDifference(msg, leftColumn.isKeyColumn(), rightColumn.isKeyColumn());
            return false;
        }

        return true;
    }

    private void addDifference(String msg, Object... args) {
        String diffMsg = MessageFormat.format(msg, args);
        final String compareString = String.format("Columns: '%s' vs. '%s': '%s'", leftColumn, rightColumn, diffMsg);
        differences.add(compareString + diffMsg);
    }
}
