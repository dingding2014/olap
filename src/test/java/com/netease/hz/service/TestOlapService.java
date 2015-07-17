package com.netease.hz.service;

import com.google.gson.Gson;
import com.netease.hz.model.Query;
import com.netease.hz.utils.Props;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhifei on 3/26/15.
 */
public class TestOlapService {
    @Before
    public void init (){
        //Props.initInstance("/Users/zhifei/Desktop/distributedstack/OLAPPlatform/src/main/resources/application.properties");
    	Props.initInstance("C:\\Users\\Administrator.QH-20141210BDBI\\tutorial\\src\\main\\resources\\application.properties");
    }

    @Test
    public void testExecuteSync(){
        String mdx = "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS, " +
                "{([Promotion Media].[All Media], [Product].[All Products])} ON ROWS " +
                "from [Sales] " +
                "where [Time].[1997]";
        OlapService s = new OlapService();
        String result = s.executeSync("HR", mdx);
        System.out.println(result);
    }

    @Test
    public void testExecuteAsyn() {
    	/*
        String mdx = "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS, " +
                "{([Promotion Media].[All Media], [Product].[All Products])} ON ROWS " +
                "from [Sales] " +
                "where [Time].[1997]";*/
    	String mdx = "SELECT "
        		+ "{[Measures].[sale_count],[Measures].[ave_price],[Measures].[sale_totall]} on 0, "
        		+ "{[customer_gender].[F],[customer_gender].[M],[customer_gender].[all_gender]} on 1 "
        		//+ "{[city].[Beijing],[city].[Shanghai],[city].[Hangzhou],[city].[all_city]} on 2 "
        		+ "from [dc_test]";
    	String mdx1 = "SELECT "+
" {[Measures].[sale_count], [Measures].[ave_price], [Measures].[sale_totall]} ON COLUMNS, "+
"Order(TopCount({[customer_gender].[F], [customer_gender].[M]}, [Measures].[sale_count]>166 or [Measures].[ave_price]>0, 2, [Measures].[sale_count]), [Measures].[sale_totall], ASC) ON ROWS"
+" FROM [dc_test]";
        OlapService s = new OlapService();
        long id = s.executeAsyn("dc_test", mdx1);
        System.out.println("id=" + id);

        Query q = s.getQueryById(id);

        System.out.println("gson= :"+new Gson().toJson(q));
    }
}
