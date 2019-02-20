package de.areto.datachef.model.mart;

import de.areto.datachef.model.compilation.CompilationUnit;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.*;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class Mart extends CompilationUnit {

    public Mart(String name) {
        super(name);
    }

    @Enumerated(value = EnumType.STRING)
    private MartType martType;

    @OneToMany(mappedBy = "parentMart", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<MartColumn> columns = new HashSet<>();

    public List<MartColumn> getMartColumnsOriginalOrder() {
        return columns.stream().sorted(Comparator.comparing(MartColumn::getOrderNumber))
                .collect(Collectors.toList());
    }

    public void addMartColumn(@NonNull MartColumn martColumn) {
        martColumn.setParentMart(this);
        martColumn.setOrderNumber(this.columns.size()+1);
        this.columns.add(martColumn);
    }
}
