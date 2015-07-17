package com.netease.hz.dao;

import com.netease.hz.model.Datacube;
import com.netease.hz.utils.Props;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhifei on 3/26/15.
 */
public class TestCubeLoader {

    DatacubeLoader loader;
    @Before
    public void init (){
        Props.initInstance("C:\\Users\\Administrator.QH-20141210BDBI\\tutorial\\src\\main\\resources\\application.properties");
        loader = new DatacubeLoader();
    }

    @Test
    public void testGetDatacubeByName(){
        Datacube datacube = loader.getDatacubeByName("dc_test");
        //System.out.println(datacube.getXml());
        System.out.println(datacube.getDatasource());

    }

}
