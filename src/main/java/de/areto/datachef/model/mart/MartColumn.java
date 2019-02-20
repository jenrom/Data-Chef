package de.areto.datachef.model.mart;

import de.areto.datachef.model.common.IdentifiableEntity;
import de.areto.datachef.model.datavault.DataDomain;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.ManyToOne;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class MartColumn extends IdentifiableEntity {

    @ManyToOne(fetch = FetchType.LAZY)
    @NaturalId
    private Mart parentMart;

    @ManyToOne
    private DataDomain dataDomain;

    private boolean keyColumn;

    private boolean identityColumn;

    private String comment;

    private int orderNumber;

    public boolean hasComment() {
        return checkNullOrEmpty(comment);
    }

    private boolean checkNullOrEmpty(String string) {
        return string != null && !string.isEmpty();
    }

}
