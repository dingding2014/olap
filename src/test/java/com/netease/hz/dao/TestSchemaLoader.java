package com.netease.hz.dao;

import com.netease.hz.model.Query;
import com.netease.hz.model.Schema;
import com.netease.hz.utils.Props;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhifei on 3/25/15.
 */
public class TestSchemaLoader {
    SchemaLoader loader;

    @Before
    public void init (){
        Props.initInstance("/Users/zhifei/Desktop/distributedstack/OLAPPlatform/src/main/resources/application.properties");
        loader = new SchemaLoader();
    }

    @Test
    public void  testGetSchema() throws Exception{
       Schema schema = loader.getSchema("foodmart.xml");

        System.out.println(schema.getFileLocation());
    }

}
