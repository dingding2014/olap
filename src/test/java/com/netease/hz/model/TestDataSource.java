package com.netease.hz.model;

import java.util.Properties;

import org.junit.Test;

import com.netease.hz.service.DataSourceService;
import com.netease.hz.utils.Props;

/**
 * Created by zhifei on 3/18/15.
 */
public class TestDataSource {

    @Test
    public void testMethod() {
    	Props.initInstance("C:\\Users\\Administrator.QH-20141210BDBI\\tutorial\\src\\main\\resources\\application.properties");
    	/*
    	DataSource ds=new DataSource();
    	ds.setName("datasource");
    	ds.setType("mysql");
    	ds.setHost("localhost");
    	ds.setPort("3306");
    	ds.setDatabase("datasource");
    	ds.setUsername("root");
    	ds.setPassword("root");
    	ds.setDrivername("mysql");
    	Properties properties = new Properties();
    	properties.setProperty("telephone", "1234");
    	properties.setProperty("id", "214");
    	ds.setPara(properties);
        System.out.println(ds.getUrl());*/
    	DataSourceService ds_service=new DataSourceService();
        //ds_service.addDataSource(ds);
    	boolean flag;
        flag = ds_service.deleteDataSource("ds_test");
        if(flag) System.out.println("yes");
        else System.out.println("no");
    	//getDataSourceByName("datasource");
    }

}
