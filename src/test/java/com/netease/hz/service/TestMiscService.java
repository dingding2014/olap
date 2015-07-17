package com.netease.hz.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.netease.hz.dao.DatacubeLoader;
import com.netease.hz.model.Datacube;
import com.netease.hz.utils.Props;

public class TestMiscService {
	
	 MiscService misc_service = null;
	 
	 @Before
	 public void init () {		 
	     Props.initInstance("C:\\Users\\Administrator.QH-20141210BDBI\\tutorial\\src\\main\\resources\\application.properties");
	     misc_service = new MiscService();
	 }

	 @Test
	 public void testgetLevelMembers() {
		 /*
		 String cube_name = "dc_test";
		 String dimension_name = "product_type";
		 String level_name = "proId";	 
		 System.out.println("Cube name: "+cube_name);
		 System.out.println("Dimension name: "+dimension_name);
		 System.out.println("Level name: "+level_name);
		 
		 int count = misc_service.getLevelMemberCount(cube_name, dimension_name, level_name);
		 System.out.println("该级别下的成员数目为: "+count);
	     List<String> member_name = misc_service.getLevelMembers(cube_name, dimension_name, level_name, 0, 4);
	     System.out.print("该级别下的成员为： ");
	     Gson gson = new Gson();
	     System.out.println(gson.toJson(member_name));*/
		 Integer limit = null;
		 if(limit==null) limit = 3;
		 System.out.println(limit);
	 }

}
