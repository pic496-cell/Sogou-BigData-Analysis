package com.djt.flink.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author dajiangtai
 * @create 2020-01-24-12:30
 */
public class Test {
    public static void main(String[] args) {
        List<UserHourPackageKey> list = new ArrayList();

        UserHourPackageKey key = new UserHourPackageKey();
        key.setUserId(101L);
        key.setHour("06");
        key.setPackageName("bb");
        list.add(key);

        UserHourPackageKey key2 = new UserHourPackageKey();
        key2.setUserId(101L);
        key2.setHour("06");
        key2.setPackageName("dd");
        list.add(key2);

        UserHourPackageKey key3 = new UserHourPackageKey();
        key3.setUserId(101L);
        key3.setHour("09");
        key3.setPackageName("aa");
        list.add(key3);

        Collections.sort(list);

        for (UserHourPackageKey uhp:list){
            System.out.println(uhp.getUserId()+"-"+uhp.getHour()+"-"+uhp.getPackageName());
        }
    }
}
