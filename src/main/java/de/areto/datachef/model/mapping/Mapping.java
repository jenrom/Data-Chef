package de.areto.datachef.model.mapping;

import de.areto.datachef.model.compilation.CompilationUnit;
import de.areto.datachef.model.datavault.DVObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.*;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Entity
@Getter
@Setter
@NoArgsConstructor
@NamedEntityGraph(
        name = "graph-mapping-full",
        attributeNodes = {
                @NamedAttributeNode("expressions"),
                @NamedAttributeNode("mappingColumns"),
                @NamedAttributeNode("dependencyList"),
                @NamedAttributeNode("csvType"),
                @NamedAttributeNode(value = "mappingObjectReferences", subgraph = "sub-graph-object-references-full")
        },
        subgraphs = {
                @NamedSubgraph(
                        name = "sub-graph-object-references-full",
                        attributeNodes = {
                                @NamedAttributeNode(value = "object", subgraph = "subgraph-object-full")
                        }
                ),
                @NamedSubgraph(
                        name = "subgraph-object-full",
                        attributeNodes = {
                                @NamedAttributeNode("columns"),
                                @NamedAttributeNode("comments")
                        }
                ),
        }
)
public class Mapping extends CompilationUnit {

    public Mapping(String name) {
        super(name);
    }

    private String connectionName;

    @Enumerated(EnumType.STRING)
    private ConnectionType connectionType;

    @Enumerated(EnumType.STRING)
    private StagingMode stagingMode;

    @ManyToOne
    private CsvType csvType;

    private boolean fullLoad;

    @OneToMany(mappedBy = "mapping", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<MappingColumn> mappingColumns = new HashSet<>();

    @OneToMany(mappedBy = "mapping", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<MappingObjectReference> mappingObjectReferences = new HashSet<>();

    @Transient
    private Set<DVObject> newObjects = new HashSet<>();

    public void setMappingColumns(Set<MappingColumn> mappingColumns) {
        this.mappingColumns.clear();
        this.mappingColumns.addAll(mappingColumns);
    }

    public void setMappingObjectReferences(Set<MappingObjectReference> mappingObjectReferences) {
        this.mappingObjectReferences.clear();
        this.mappingObjectReferences.addAll(mappingObjectReferences);
    }

    public void addNewObject(@NonNull DVObject object) {
        newObjects.add(object);
    }

    public void addMappingColumn(@NonNull MappingColumn mappingColumn) {
        mappingColumn.setMapping(this);
        mappingColumn.setOrderPosition(mappingColumns.size() + 1);
        this.mappingColumns.add(mappingColumn);
    }

    public void addObject(@NonNull DVObject object) {
        final MappingObjectReference reference = new MappingObjectReference(this, object);
        this.mappingObjectReferences.add(reference);
    }

    public void addObjectWithRole(@NonNull DVObject object, String role) {
        final MappingObjectReference reference = new MappingObjectReference(this, object, role);
        this.mappingObjectReferences.add(reference);
    }

    public Collection<DVObject> getMappedObjects() {
        return mappingObjectReferences.stream().map(MappingObjectReference::getObject).collect(Collectors.toSet());
    }

    public Collection<MappingObjectReference> getMappingObjectReferencesSorted() {
        return mappingObjectReferences.stream().sorted().collect(Collectors.toList());
    }

    public Collection<MappingColumn> getMappingColumnsSorted() {
        return mappingColumns.stream().sorted(Comparator.comparing(MappingColumn::getFinalName))
                .collect(Collectors.toList());
    }

    public Collection<MappingColumn> getMappingColumnsOriginalOrder() {
        return mappingColumns.stream().sorted(Comparator.comparing(MappingColumn::getOrderPosition))
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "Mapping{" + getName() + (isTransient() ? "transient" : "" ) + '}';
    }
}