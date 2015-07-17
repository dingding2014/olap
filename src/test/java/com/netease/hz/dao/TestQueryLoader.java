package com.netease.hz.dao;

import com.google.gson.Gson;
import com.netease.hz.model.Query;
import com.netease.hz.utils.Props;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by zhifei on 3/25/15.
 */
public class TestQueryLoader {
    @Before
    public void init (){
        Props.initInstance("/Users/zhifei/Desktop/distributedstack/OLAPPlatform/src/main/resources/application.properties");
    }

    @Test
    public void  testGetResultByQueryId() throws Exception{
        QueryLoader loader = new QueryLoader();
        assert (loader.getResultByQueryId(2) != null);
    }

    @Test
    public void  testGetQueryById() throws Exception{
        QueryLoader loader = new QueryLoader();
        Query q = loader.getQueryById(8);
        System.out.println(q.getResult());
        System.out.println(new Gson().toJson(q));
    }

    @Test
    public void testUpdateQuery() {
        QueryLoader loader = new QueryLoader();
        Query q = new Query(000,"testquer",0);
        q.setEndTime(111);
        q.setState(1);
        q.setResult("done!");
        loader.updateQuery(2, q);
    }

    @Test
    public void testAddQuery()  {
        QueryLoader loader = new QueryLoader();
        Query q = new Query(000,"testquer",0);
        long id = loader.addQuery(q.getQuery(),q.getStartTime(), q.getState());
        System.out.println(id);
    }
}
