package de.areto.datachef.model.mapping;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Objects;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class CsvType {

    @Id
    private String name;
    private String encoding;
    private String columnDelimiter;
    private String columnSeparator;
    private String rowSeparator;
    private int skip;
    private int rejectLimit;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CsvType csvType = (CsvType) o;
        return Objects.equals(name, csvType.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
