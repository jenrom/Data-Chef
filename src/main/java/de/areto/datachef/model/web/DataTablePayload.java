package de.areto.datachef.model.web;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class DataTablePayload<T> {

    private int draw;
    private long recordsTotal;
    private long recordsFiltered;
    private List<T> data = new ArrayList<>();
    private String error;

}
