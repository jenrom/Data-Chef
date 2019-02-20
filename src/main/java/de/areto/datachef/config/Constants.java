package de.areto.datachef.config;

import lombok.experimental.UtilityClass;

import java.time.format.DateTimeFormatter;

/**
 * Utility class that holds project wide constants.
 */
@UtilityClass
public class Constants {

    public static final DateTimeFormatter DF_PUB_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final int CARGO_MSG_SIZE = 1000;
    public static final int DV_MAX_CMNT_SIZE = 1000;

    public static final String COL_CMNT_LOAD_ID = "Load id of the ETL process";
    public static final String COL_CMNT_LAST_SEEN = "Last Seen timestamp";
    public static final String COL_CMNT_LOAD_DATE_FROM = "Technical Load Date (from)";
    public static final String COL_CMNT_LOAD_DATE_END = "Technical Load Date (to)";

    public static final String SINK_FILE_NAME_PATTERN = "(\\d{8}){1}(\\_\\d{4,6})?";

    public static final String USER_SESSION_ATTRIBUTE_NAME = "user";
    public static final String DB_OK = "dbok";

    public static final String DATA_CHEF_BASE_PACKAGE = "de.areto.datachef";
    public static final int STARTUP_ERR_DWH_UNREACHABLE = 1;
    public static final int STARTUP_ERR_DWH_UNHEALTHY = 2;
    public static final int STARTUP_ERR_DWH_SETUP = 3;
    public static final int STARTUP_ERR_REPO_UNREACHABLE = 1;
    public static final int STARTUP_ERR_REPO_UNHEALTHY = 2;
    public static final int STARTUP_ERR_REPO_SETUP = 3;
}
