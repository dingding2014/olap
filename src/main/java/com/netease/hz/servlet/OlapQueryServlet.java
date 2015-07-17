package com.netease.hz.servlet;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.netease.hz.service.OlapService;
import com.netease.hz.service.CommonQueryService;

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
 * Handle the mdx query, including synchronized way and asynchronous.
 * Created by zhifei on 3/26/15.
 */
public class OlapQueryServlet extends HttpServlet {
    private static final Logger logger = Logger.getLogger(OlapQueryServlet.class);
    private OlapService olapService = null;
    private Gson gson;

    @Override
    public void init() throws ServletException {
        super.init();
        olapService = new OlapService();
        gson = new Gson();
    }


    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	//solve the problem of cross-domain
    	response.setHeader("Access-Control-Allow-Origin", "*");
    	response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
		
        String method = this.getMethod(request);
        PrintWriter out = response.getWriter();
        if (method.equalsIgnoreCase("getResult")) {
            String queryid = request.getParameter("queryId");
            logger.info("method:getResult, queryid=" + queryid);
            long id;
            try {
                id = Long.valueOf(queryid);
            } catch (NumberFormatException e) {
                out.write("The " + queryid + " is not a number");
                return;
            }
            out.write(gson.toJson(olapService.getQueryById(id)));
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	//solve the problem of cross-domain
    	response.setHeader("Access-Control-Allow-Origin", "*");
    	response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
    	
    	String method = this.getMethod(request);
    	
        String jsonPara = null;
        try {
            jsonPara = new String(this.getContent(request));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        Map<String, String> para = new HashMap<String, String>();
        Map<String, Object> map = gson.fromJson(jsonPara, new TypeToken<Map<String, Object>>(){}.getType()); 
        for (String key : map.keySet()) {
        	para.put(key, map.get(key).toString());
        }
        PrintWriter out = response.getWriter();
       
        if (method.equalsIgnoreCase("syncMdxQuery")) {

            String mdx = para.get("mdx");
            String cubeName = para.get("cubeName");
            if (mdx == null || cubeName == null) {
                logger.error("Parameter is too few. mdx=" + mdx + ",cubeName=" + cubeName);
                return;
            }
            logger.info("method:syncMdxQuery, mdx=" + mdx + ",cubeName=" + cubeName);
            String result = olapService.executeSync(cubeName, mdx);
            out.write(result);

        } else if (method.equalsIgnoreCase("mdxQuery")) {
            String mdx = para.get("mdx");
            String cubeName = para.get("cubeName");
            if (mdx == null || cubeName == null) {
                logger.error("Parameter is too few. mdx=" + mdx + ",cubeName=" + cubeName);
                return;
            }
            logger.info("method:mdxQuery, mdx=" + mdx + ",cubeName=" + cubeName);
            Map<String, Long> res = new HashMap<String, Long>();
            long id = olapService.executeAsyn(cubeName, mdx);
            res.put("queryId", id);
            out.write(gson.toJson(res));
        } else if(method.equalsIgnoreCase("commonQuery")) {
        	CommonQueryService commonQueryService = new CommonQueryService();   	
        	String mdx = commonQueryService.getMDX(para);
        	String cubeName = para.get("cube");
        	Map<String, String> res = new HashMap<String, String>();
            long id = olapService.executeAsyn(cubeName, mdx);
            res.put("mdx", mdx);
            res.put("queryId", Long.toString(id));
            out.write(gson.toJson(res));
        } else if(method.equalsIgnoreCase("simpleQuery")) {
        	CommonQueryService commonQueryService = new CommonQueryService();   	
        	String mdx = commonQueryService.getMDX(para);
        	String cubeName = para.get("cube");
        	Map<String, List<List<String>>> res = new HashMap<String, List<List<String>>>();        
        	List<List<String>> result = commonQueryService.simpleQuery(cubeName, mdx);      
            res.put("result",result);
            out.write(gson.toJson(res));
        }
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

    private String getMethod(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String method = uri.substring(uri.lastIndexOf("/") + 1);
        return method;
    }
}
