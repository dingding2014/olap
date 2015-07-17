package com.netease.hz.utils;


import com.google.gson.Gson;
import org.apache.hadoop.util.hash.Hash;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.Test;
import org.olap4j.*;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;

/**
 * Created by zhifei on 3/19/15.
 */
public class TestDom4j {

    @Test
    public void testDom4j() throws DocumentException {
        String xml = "/Users/zhifei/Documents/footmart.xml";

        SAXReader reader = new SAXReader();
        Document doc = reader.read(new File(xml));

        Element schema = doc.getRootElement(); // 获取根节点

        List nodes = schema.selectNodes("/Schema/Cube");

        for (Iterator it = nodes.iterator(); it.hasNext();) {
            Element elm = (Element) it.next();
            System.out.println("name:"+elm.attributeValue("name"));

        }

    }

    @Test
    public void testGetFileNameFromPath() {
        String filepath = "/Users/zhifei/Desktop/distributedstack/OLAPPlatform/target/com.netease.hz.olap-1.0-SNAPSHOT/WEB-INF/classes/schema/footmart.tzr";

        System.out.println(filepath.endsWith("xml"));
    }

    @Test
    public void testCellSet() {

        TestClass test = new TestClass();

        Set<String> set = new HashSet<String>();
        set.add("str1");
        set.add("str2");
        set.add("str3");

        test.maps.put("1", set);

        Set<String> set2 = new HashSet<String>();
        set2.add("str1");
        set2.add("str2");
        set2.add("str3");
        test.maps.put("2", set2);

        Gson gson = new Gson();
        System.out.println(gson.toJson(test));


    }

    @Test
    public void testList2Array() {
        List<String> list = new ArrayList<String>();
        list.add("1");
        list.add("2");

        String[] ary = new String[0];
        ary = list.toArray(ary);
        for(String s : ary) {
            System.out.println(s);
        }
    }

}

class TestClass {
    public Map<String, Set<String>> maps = new HashMap<String, Set<String>>();
    public double value;


}
