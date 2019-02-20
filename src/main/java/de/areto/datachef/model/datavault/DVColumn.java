package de.areto.datachef.model.datavault;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import de.areto.datachef.config.Constants;
import de.areto.datachef.model.common.IdentifiableEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.ManyToOne;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@Entity
@Getter
@Setter
@NoArgsConstructor
@JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class)
public class DVColumn extends IdentifiableEntity implements Serializable {

    @ManyToOne
    @NaturalId
    private DVObject parent;

    @ManyToOne(optional = false)
    private DataDomain dataDomain;

    private boolean keyColumn = false;

    @ElementCollection
    @Column(length = Constants.DV_MAX_CMNT_SIZE)
    private Set<String> comments = new HashSet<>();

    public boolean hasComment() {
        return !this.comments.isEmpty();
    }

    public void addComment(String comment) {
        checkArgument(comment.length() <= Constants.DV_MAX_CMNT_SIZE,
                "Comment is longer than 1000 characters");
        this.comments.add(comment);
    }

    @Override
    public String toString() {
        return String.format("DVColumn{%s (%s), kc=%s}", getName(), dataDomain, keyColumn);
    }
}
