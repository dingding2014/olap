package com.netease.hz.service;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.netease.hz.model.Datacube;
import com.netease.hz.utils.Props;

public class TestDatacubeService {

	 DatacubeService dc_service = null;
	 
	 @Before
	 public void init () {		 
	     Props.initInstance("C:\\Users\\Administrator.QH-20141210BDBI\\tutorial\\src\\main\\resources\\application.properties");
	     dc_service = new DatacubeService();
	 }

	 @Test
	 public void testGetcubeModelByName() {
		 Datacube dc = new Datacube();
	     dc = dc_service.getDatacubeModelByName("dc_test");
	     Map<String,Datacube> map = new HashMap<String,Datacube>();
	     map.put("cube", dc);
	     Gson gson = new Gson();
	     System.out.println(gson.toJson(map));
	 }

}
