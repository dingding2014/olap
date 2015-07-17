package com.netease.hz.servlet;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.netease.hz.dao.DatasourceLoader;
import com.netease.hz.model.DataSource;
import com.netease.hz.model.Datacube;
import com.netease.hz.service.DataSourceService;



import com.netease.hz.service.DatacubeService;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * Created by zhuqinghua on 3/18/15.
 */
public class DataSourceServlet extends HttpServlet {
	private static final Logger logger = Logger.getLogger(DataSourceServlet.class);
	DataSourceService ds_service;
	Gson gson = null;

	@Override
	public void init() throws ServletException {
		super.init();
		ds_service = new DataSourceService();
		gson = new Gson();
	}


	protected void doGet(HttpServletRequest request,HttpServletResponse response) throws ServletException, IOException {
	
		//solve the problem of cross-domain
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
		
		String request_url = request.getRequestURI();     
        PrintWriter out = response.getWriter();
           
        if(request_url.indexOf("getAll")>=0) {
        	String pattern = null;   
        	pattern = request.getParameter("pattern");
        	Map<String,List<String>> DataSourceName = new HashMap<String,List<String>>();
        	DataSourceName = ds_service.getAllDataSource(pattern);
            out.write(gson.toJson(DataSourceName));
        } else if(request_url.indexOf("getDatasource")>=0) {
        	String name = request.getParameter("name").toString();  
        	DataSource ds = ds_service.getDataSourceByName(name);
        	out.write(gson.toJson(ds));
        } else if(request_url.indexOf("getTables")>=0) {
        	String name = request.getParameter("name");
        	Map<String,List<String>> table_name = new HashMap<String,List<String>>();
        	table_name = ds_service.getTableofDatasource(name);
        	out.write(gson.toJson(table_name));
        } else if(request_url.indexOf("tableSchema")>=0) {
        	String name = request.getParameter("name");
        	String table = request.getParameter("table");
        	Map<String,List<Map<String,String>>> list = new HashMap<String,List<Map<String,String>>>();
        	list = ds_service.getFieldsofTable(name,table);
        	out.write(gson.toJson(list));
        }
	}

	protected void doPost(HttpServletRequest request,HttpServletResponse response) throws ServletException, IOException {
		
		//solve the problem of cross-domain
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
		
		byte[] buffer = null;
		try {
			buffer = this.getContent(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String request_json = new String(buffer);
		Map<String, Object> map = gson.fromJson(request_json, new TypeToken<Map<String, Object>>(){}.getType());  
		PrintWriter out = response.getWriter();
		boolean flag = false;
		DatasourceLoader loader = null;
	    DataSource ds = null;
	    String request_url = request.getRequestURI(); 
	    Map<String, String> results = new HashMap<String, String>(); 
		if ( request_url.indexOf("add")>=0) {
			
			loader = new DatasourceLoader();
			ds = loader.getDatasourceFromMap(map);
			flag=ds_service.addDataSource(ds);
			if(!flag) results.put("message", "datasource add error");
		} else if (request_url.indexOf("modify")>=0) {
			
			loader = new DatasourceLoader();
			ds = loader.getDatasourceFromMap(map);
			flag=ds_service.modifyDataSource(ds);
			if(!flag) results.put("message", "datasource modify error");
		} else if (request_url.indexOf("delete")>=0) {
			
			String name = map.get("name").toString();
			String deleteCube = map.get("deleteCube").toString();	
			flag=ds_service.deleteDataSource(name);
			if(flag) {
				if(deleteCube.equalsIgnoreCase("ture")) {				
				    DatacubeService dc_service = new DatacubeService();
				    List<Datacube> datacubes = dc_service.getCubesByDatasource(name);
				    for(Datacube dc : datacubes) {
					    flag = dc_service.deleteDatacube(dc.getName());
					    if(!flag) break;
				    }
				}
			}
			
			if(!flag) results.put("message", "datasource delete error");		
		} 
		if(flag) results.put("code", "200");
		else {
			results.put("code", "500");
		}
		out.write(gson.toJson(results));
	}
	
	private byte[] getContent(HttpServletRequest request) throws Exception {
        int len = 0;
        InputStream is = null;
        len = request.getContentLength();
        if (len == 0)
            throw new Exception("content length is 0");
        is = request.getInputStream();
        try {
            if (len == -1)
                throw new IOException("read content length is -1!");
            byte[] buffer = new byte[len];
            if (is != null)
                IOUtils.readFully(is, buffer);
            logger.info("servlet:" + request.getServletPath() + ",params:" + new String(buffer));
            return buffer;
        } catch (Exception ex) {
            logger.error(ex);
            throw ex;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

}
