package de.areto.datachef.model.template;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class HashColumn {

    private final String name;

    private List<String> hashColumns = new ArrayList<>();
}
