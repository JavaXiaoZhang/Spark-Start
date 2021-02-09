package com.zq.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Objects;

public class UAUtils {

    private static UASparser parser;

    static {
        try {
            parser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static com.imooc.bigdata.domain.UserAgentInfo getUserAgentInfo(String ua) {
        if (StringUtils.isBlank(ua)) {
            return null;
        }
        com.imooc.bigdata.domain.UserAgentInfo info = null;
        try {
            UserAgentInfo tmp = parser.parse("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; )");
            if (!Objects.isNull(tmp)) {
                info = new com.imooc.bigdata.domain.UserAgentInfo();
                info.setBrowserName(tmp.getUaFamily());
                info.setBrowserVersion(tmp.getBrowserVersionInfo());
                info.setOsName(tmp.getOsFamily());
                info.setOsVersion(tmp.getOsName());
            }
            System.out.println(info);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return info;
    }

    public static void main(String[] args) {
        String ua = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; )";
        com.imooc.bigdata.domain.UserAgentInfo userAgentInfo = getUserAgentInfo(ua);
    }
}
