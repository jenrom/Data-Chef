package de.areto.datachef.model.mapping;

import de.areto.datachef.config.Constants;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.model.common.IdentifiableEntity;
import de.areto.datachef.model.datavault.DataDomain;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MappingColumn extends IdentifiableEntity implements Serializable {

    @ManyToOne(fetch = FetchType.LAZY)
    @NaturalId
    private Mapping mapping;

    @ManyToOne(optional = false)
    private DataDomain dataDomain;

    private String newName;

    @Column(length = Constants.DV_MAX_CMNT_SIZE)
    private String comment;

    @Column(length = Constants.DV_MAX_CMNT_SIZE)
    private String objectComment;

    private String objectName;
    private String objectAlias;

    @Column(length = 1000)
    private String calculation;
    private String satelliteName;

    private int orderPosition;

    @Transient
    private String dataDomainName;

    private String linkSatelliteName;
    private String roleName;

    private boolean partOfLinkSatellite;
    private boolean keyColumn;
    private boolean ignored;

    @OneToMany(mappedBy = "mappingColumn", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<MappingColumnReference> mappingColumnReferences = new HashSet<>();

    public void addMappingColumnReference(MappingColumnReference reference) {
        reference.setMappingColumn(this);
        this.mappingColumnReferences.add(reference);
    }

    public void setMappingColumnReferences(Set<MappingColumnReference> mappingColumnReferences) {
        this.mappingColumnReferences.clear();
        this.mappingColumnReferences.addAll(mappingColumnReferences);
    }

    public boolean isPartOfHubSatellite() {
        return !keyColumn && !isPartOfLinkSatellite() && !isIgnored();
    }

    public String getFinalName() {
        return hasNewName() ? newName : getName();
    }

    public String getFinalNameAndRole() {
        final StringBuilder b = new StringBuilder();
        b.append(hasNewName() ? newName : getName());
        if(hasRoleName())
            b.append("_").append(roleName);
        return b.toString();
    }

    public String getCookedTableName() {
        final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);
        final String prefix;

        if(isPartOfHubSatellite())
            prefix = dvConfig.satNamePrefix() + (hasSatelliteName() ? getSatelliteName() : getObjectName());
        else if(isPartOfLinkSatellite())
            prefix = dvConfig.satNamePrefix() + (hasSatelliteName() ?  getSatelliteName() : getLinkSatelliteName());
        else
            prefix = dvConfig.hubNamePrefix() + getObjectName();

        return  prefix + "_" + getFinalNameAndRole();
    }

    public boolean hasNewName() {
        return checkNullOrEmpty(newName);
    }

    public boolean hasComment() {
        return checkNullOrEmpty(comment);
    }

    public boolean hasObjectComment() {
        return checkNullOrEmpty(objectComment);
    }

    public boolean hasObjectName() {
        return checkNullOrEmpty(objectName);
    }

    public boolean hasObjectAlias() {
        return checkNullOrEmpty(objectAlias);
    }

    public boolean hasCalculation() {
        return checkNullOrEmpty(calculation);
    }

    public boolean hasDataDomainName() {
        return checkNullOrEmpty(dataDomainName);
    }

    public boolean hasSatelliteName() {
        return checkNullOrEmpty(satelliteName);
    }

    public boolean hasLinkSatelliteName() {
        return checkNullOrEmpty(linkSatelliteName);
    }

    public boolean hasRoleName() {
        return checkNullOrEmpty(roleName);
    }

    private boolean checkNullOrEmpty(String string) {
        return string != null && !string.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("MappingColumn{%s%s}", getFinalName(), isTransient() ? " (transient)" : "");
    }
}
