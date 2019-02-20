package de.areto.datachef.config;

import de.areto.datachef.model.datavault.DVObject;
import lombok.experimental.UtilityClass;
import org.aeonbits.owner.ConfigCache;

@UtilityClass
public class ConfigUtility {

    private static DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);

    public static String getKeyDomainName(DVObject o) {
        if(o.getType().equals(DVObject.Type.SAT)) {
            return dvConfig.hubKeyDataDomain();
        } else if(o.getType().equals(DVObject.Type.HUB)) {
            return dvConfig.hubKeyDataDomain();
        } else {
            return dvConfig.linkKeyDataDomain();
        }
    }

    public static String getNamePrefix(DVObject o) {
        if(o.getType().equals(DVObject.Type.SAT)) {
            return dvConfig.satNamePrefix();
        } else if(o.getType().equals(DVObject.Type.HUB)) {
            return dvConfig.hubNamePrefix();
        } else {
            return dvConfig.linkNamePrefix();
        }
    }

    public static String getKeySuffix(DVObject o) {
        if(o.getType().equals(DVObject.Type.SAT)) {
            return dvConfig.hubKeySuffix();
        } else if(o.getType().equals(DVObject.Type.HUB)) {
            return dvConfig.hubKeySuffix();
        } else {
            return dvConfig.linkKeySuffix();
        }
    }

}
