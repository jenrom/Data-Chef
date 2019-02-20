package de.areto.datachef.model.web;

import lombok.Data;
import lombok.NonNull;
import spark.Request;

import java.util.Objects;

@Data
public class DataTableCallback {

    private String orderDirParam;
    private String searchParam;
    private int draw;
    private int start;
    private int length;
    private int orderIndex;

    public static DataTableCallback fromRequest(@NonNull Request request) {
        final DataTableCallback callback = new DataTableCallback();
        callback.setOrderDirParam(Objects.toString(request.queryParams("order[0][dir]"), "desc"));
        callback.setSearchParam(Objects.toString(request.queryParams("search[value]"), ""));
        callback.setDraw(Integer.valueOf(Objects.toString(request.queryParams("draw"), "1")));
        callback.setStart(Integer.valueOf(Objects.toString(request.queryParams("start"), "0")));
        callback.setLength(Integer.valueOf(Objects.toString(request.queryParams("length"), "10")));
        callback.setOrderIndex(Integer.valueOf(Objects.toString(request.queryParams("order[0][column]"), "0")));
        return callback;
    }

    public int getPage() {
        return start > 0 ? start / length : 0;
    }
}
