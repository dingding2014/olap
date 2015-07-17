package com.netease.hz.model;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;



public class TestJsonArray {
	
	public static void main(String[] args) {        
          Gson gson=new Gson(); 
          /*
          List<String> table_name=new ArrayList<String>();
          table_name.add("test1");
          table_name.add("test2");
          String s=gson.toJson(table_name);
          List<Map<String,String>> list=new ArrayList<Map<String, String>>();
          Map<String,String> map=new HashMap<String,String>();
          map.put("name", "zqh");
          map.put("type", "whaha");
          list.add(map);
          //map.put("name", "pzf");
          //list.add(map);
          //String s=gson.toJson(list);
          System.out.println(s);
          //String json = "{'dimension':'city','level':'city_name','numbers':'['2014','2015']'}";
          //gson.fromJson(json, (Type) new QueryCondition());
          QueryCondition queryCondition = new QueryCondition();
          queryCondition.setDimension("city");
          queryCondition.setLevel("city_name");
          List<String> members = new ArrayList<String>();
          members.add("2014");
          members.add("2015");
          queryCondition.setMembers(members);
          QueryCondition queryCondition1 = new QueryCondition();
          queryCondition1.setDimension("city1");
          queryCondition1.setLevel("city_name1");
          List<QueryCondition> list1 = new ArrayList<QueryCondition>();
          list1.add(queryCondition);
          list1.add(queryCondition1);
          String s2 = gson.toJson(list1);
          System.out.println("s2= "+s2);
          Map<String, List<QueryCondition>> mapp = new HashMap<String, List<QueryCondition>>();
          mapp.put("test", list1);
          System.out.println(gson.toJson(mapp));
          //QueryCondition qc = gson.fromJson(s2, new TypeToken<QueryCondition>(){}.getType());
          //System.out.println(qc.getDimension());
          String s3 = "[{\"dimension\":\"city\",\"level\":\"city_name\"},{\"dimension\":\"city1\",\"level\":\"city_name1\"}]";
          String s4 = "[[{'dimension':'city','level':'city_name'}],[{'dimension':'city1','level':'city_name1','members':['2015','2016']}]]";
          String json1 = "{\"k2\":\"v2\",\"k1\":\"v1\",\"大神\":\"塔下\"}";
          //String json = "{\"dimension\":\"city\",\"level\":\"city_name\"}";
          List<QueryCondition> List = gson.fromJson(s4, new TypeToken<List<QueryCondition>>(){}.getType());  
          for(QueryCondition list : List)
          System.out.println(list.getMembers());
          String tmp = "[{dimension=Measures, members=[sale_count, ave_price, sale_totall]}]";
          List<QueryCondition> List = gson.fromJson(tmp, new TypeToken<List<QueryCondition>>(){}.getType());  
          for(QueryCondition list : List)
          System.out.println(list.getMembers());*/
          List<List<String>> result = new ArrayList<List<String>>();
          List<String> a1 = new ArrayList<String>();
          a1.add("1.1");
          a1.add("1.2");
          List<String> a2 = new ArrayList<String>();
          a2.add("2.1");
          a2.add("2.2");
          a2.add("2.3");
          result.add(a1);
          result.add(a2);
          System.out.println(gson.toJson(result));
     }
}
