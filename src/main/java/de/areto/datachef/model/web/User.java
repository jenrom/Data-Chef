package de.areto.datachef.model.web;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import java.util.Objects;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class User {

    public enum Role {
        ADMIN,
        DEFAULT,
        AUDITOR
    }

    @Id
    private String username;

    // MD5 Hash
    private String passwordHash;

    @Enumerated(EnumType.STRING)
    private Role role = Role.DEFAULT;

    public User(@NonNull String username, @NonNull String passwordHash) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.role = role;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(username, user.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username);
    }
}
