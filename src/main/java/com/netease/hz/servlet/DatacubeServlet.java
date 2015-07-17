package com.netease.hz.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netease.hz.model.Datacube;
import com.netease.hz.service.DatacubeService;

import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhifei on 3/19/15.
 */
public class DatacubeServlet extends HttpServlet {
    private static final Logger logger = Logger.getLogger(DatacubeServlet.class);
    DatacubeService dcservice;

    @Override
    public void init() throws ServletException {
        super.init();
        dcservice = new DatacubeService();
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        return;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	//solve the problem of cross-domain
    	response.setHeader("Access-Control-Allow-Origin", "*");
    	response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
    	
        String url = request.getRequestURI();
        
        String method = url.substring(url.lastIndexOf("/") + 1);
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        
        PrintWriter out = response.getWriter();
      
        if(method.equalsIgnoreCase("getAll")) {
            //http://localhost:8080/olap/cube/getall
            List<Datacube> cubes = dcservice.getAllDatacubes();
            Map<String, List<Datacube>> results = new HashMap<String, List<Datacube>>();
            results.put("cubes",cubes);
            out.write(gson.toJson(results));

        } else if (method.equalsIgnoreCase("getCube")){
            //http://localhost:8080/olap/cube/getCubeByName?name=Sales
            String name = request.getParameter("name");
            Datacube datacube = new Datacube();
            datacube = dcservice.getDatacubeModelByName(name);
            Map<String, Datacube> results = new HashMap<String, Datacube>();
            results.put("cube", datacube);
            out.write(gson.toJson(results));
        } else if (method.equalsIgnoreCase("deleteCube")){
            //http://localhost:8080/olap/cube/deleteCube?name=HR
            String name = request.getParameter("name");
            boolean flag = dcservice.deleteDatacube(name);
            Map<String, String> results = new HashMap<String, String>();
            if (flag) {
                results.put("code", "200");
            } else {
                results.put("code", "500");
            }
            out.write(gson.toJson(results));
        } else if (method.equalsIgnoreCase("getCubesByDatasource")){
        	
        	String name = request.getParameter("datasource");
        	List<Datacube> cubes = dcservice.getCubesByDatasource(name);
        	Map<String, List<Datacube>> results = new HashMap<String, List<Datacube>>();
            results.put("cubes",cubes);
            out.write(gson.toJson(results));
        }
    }
}
