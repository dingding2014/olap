package com.netease.hz.utils;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhifei on 3/19/15.
 */
public class TestProps {

    @Test
    public void testToJson() {
        Properties p = new Properties();
        //p.put("k1", "v1");
        //p.put("k2", "v2");  
        String value = "[\"test\"]";
        p.put("datasources", value);
        System.out.println(p);
        System.out.println(Props.toJson(p));
    }


    @Test
    public void testFromJson() {
        String json = "{\"k2\":\"v2\",\"k1\":\"v1\",\"大神\":\"塔下\"}";
        System.out.println(json);  
        Properties p = Props.getProperitesFromJson(json);
        System.out.println(p);
    }
    
    @Test
    public void testMaptoJson() {
    	List<String> name = new ArrayList<String>();
    	name.add("test1");
    	name.add("test2");
    	Map<String, List<String>> map = new HashMap<String, List<String>>();
    	//map.put("k1", "v1");
    	//map.put("k2", "v2");
    	
    	map.put("datasources", name);
    	Gson gson = new Gson();
    	System.out.println("***"+gson.toJson(map));
    	//parameters:{"k1":"v1","k2":"v2"}
    }
}
