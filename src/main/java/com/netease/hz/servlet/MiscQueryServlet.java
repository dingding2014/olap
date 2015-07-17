package com.netease.hz.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.netease.hz.model.QueryCondition;
import com.netease.hz.service.CommonQueryService;
import com.netease.hz.service.MiscService;
import com.netease.hz.utils.Props;

/**
 * Created by zhuqinghua on 4/7/15.
 */
public class MiscQueryServlet extends HttpServlet {
	private static final Logger logger = Logger.getLogger(MiscQueryServlet.class);
	MiscService misc_service = null;
	private Gson gson;

	@Override
	public void init() throws ServletException {
		// TODO Auto-generated method stub
		super.init();
		misc_service = new MiscService();
		gson = new Gson();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		// solve the problem of cross-domain
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setContentType("text/json");
		response.setCharacterEncoding("UTF-8");

		String request_url = request.getRequestURI();
		PrintWriter out = response.getWriter();
		Gson gson = new Gson();
		String cube_name = request.getParameter("cube");
		String dimension_name = request.getParameter("dimension");
		String level_name = null;
		if (request_url.indexOf("getLevelMembers") >= 0) {
			Integer offset = null, limit = null;
			if (request.getParameter("offset") != null)
				offset = Integer.parseInt(request.getParameter("offset"));
			if (request.getParameter("limit") != null)
				limit = Integer.parseInt(request.getParameter("limit"));
			List<String> member_name = misc_service.getLevelMembers(cube_name,
					dimension_name, level_name, offset, limit);
			Map<String, List<String>> map = new HashMap<String, List<String>>();
			map.put("members", member_name);
			out.write(gson.toJson(map));
		} else if (request_url.indexOf("getLevelMemberCount") >= 0) {
			level_name = request.getParameter("level");
			int count = misc_service.getLevelMemberCount(cube_name,
					dimension_name, level_name);
			Map<String, Integer> map = new HashMap<String, Integer>();
			map.put("count", count);
			out.write(gson.toJson(map));
		} else if (request_url.indexOf("getDimensionMembers") >= 0) {
			Integer offset = null, limit = null;
			if (request.getParameter("offset") != null)
				offset = Integer.parseInt(request.getParameter("offset"));
			if (request.getParameter("limit") != null)
				limit = Integer.parseInt(request.getParameter("limit"));
			List<String> member_name = misc_service.getMembersOfDimension(
					cube_name, dimension_name, offset, limit);
			Map<String, List<String>> map = new HashMap<String, List<String>>();
			map.put("members", member_name);
			out.write(gson.toJson(map));
		} else if (request_url.indexOf("getDimensionMemberCount") >= 0) {
			int count = misc_service.getMemberCountOfDimension(cube_name,
					dimension_name);
			Map<String, Integer> map = new HashMap<String, Integer>();
			map.put("count", count);
			out.write(gson.toJson(map));
		} 
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// solve the problem of cross-domain
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
		Map<String, Object> map = gson.fromJson(jsonPara,
				new TypeToken<Map<String, Object>>() {
				}.getType());
		for (String key : map.keySet()) {
			para.put(key, map.get(key).toString());
		}
		PrintWriter out = response.getWriter();

		if (method.equalsIgnoreCase("getMembersOfmultiDimensions")) {
			String cubeName = para.get("cube");
			String dimensions = para.get("dimension");
            List<String> query_dimension = gson.fromJson(dimensions, new TypeToken<List<String>>(){}.getType());
	        MiscService misc_service = new MiscService();
	        List<List<String>> result = misc_service.getMembersOfmultiDimensions(cubeName, query_dimension);
	        out.write(gson.toJson(result));
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
			logger.info("servlet:" + request.getServletPath() + ",params:"
					+ new String(buffer));
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
