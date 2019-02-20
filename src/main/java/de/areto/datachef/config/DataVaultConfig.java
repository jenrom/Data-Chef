package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.Sources;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/datavault.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface DataVaultConfig extends Accessible {

    @DefaultValue("h_")
    String hubNamePrefix();

    @DefaultValue("_key")
    String hubKeySuffix();

    @DefaultValue("chef_hash")
    String hubKeyDataDomain();

    @DefaultValue("chef_load_date")
    String loadDateName();

    @DefaultValue("chef_datetime")
    String loadDateDomain();

    @DefaultValue("chef_load_date_end")
    String loadDateEndName();

    @DefaultValue("chef_datetime")
    String loadDateEndDomain();

    @DefaultValue("9999-12-31 23:59:59.999")
    String loadDateEndInfinityValue();

    @DefaultValue("chef_last_seen_date")
    String lastSeenDateName();

    @DefaultValue("chef_datetime")
    String lastSeenDateDomain();

    @DefaultValue("chef_load_id")
    String loadIdName();

    @DefaultValue("chef_id")
    String loadIdDomain();

    @DefaultValue("chef_publish_date")
    String publishDateName();

    @DefaultValue("chef_group")
    String fileGroupName();

    @DefaultValue("s_")
    String satNamePrefix();

    // Hub's key data domain is used
    //@DefaultValue("chef_hash")
    //String satKeyDataDomain();

    @DefaultValue("_diff_hash")
    String satDiffKeySuffix();

    @DefaultValue("chef_hash")
    String satDiffKeyDataDomain();

    @DefaultValue("_hist")
    String histSatSuffix();

    @DefaultValue("l_")
    String linkNamePrefix();

    @DefaultValue("_key")
    String linkKeySuffix();

    @DefaultValue("chef_hash")
    String linkKeyDataDomain();

    @DefaultValue("###DB_ID###")
    String placeholderDbId();

    @DefaultValue("###PUBLISH_DATE###")
    String placeholderPublishDate();

    @DefaultValue("###FILE_PATH###")
    String placeholderFilePath();

    @DefaultValue("###FILE_NAME###")
    String placeholderFileName();

    @DefaultValue("###FILE_GROUP###")
    String placeholderFileGroup();

    @DefaultValue("v_")
    String viewPrefix();

    @DefaultValue("latest_")
    String viewLatestPrefix();

    @DefaultValue("all_")
    String viewAllPrefix();

    @DefaultValue("hv_")
    String histViewPrefix();

    @DefaultValue("true")
    boolean generateViews();

    @DefaultValue("true")
    boolean deleteDetectionOnFullLoad();
}
