package com.netease.hz.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Member;
import org.olap4j.query.Query;

import com.netease.hz.dao.CommonQueryLoader;
import com.netease.hz.dao.OlapLoader;
import com.netease.hz.model.Datacube;

/**
 * Created by zhuqinghua on 4/13/15.
 */
public class CommonQueryService {
	private static final Logger logger = Logger.getLogger(CommonQueryService.class);
	
	private CommonQueryLoader commonQueryloader = null;
	private OlapService olapService = null;
	private DatacubeService dc_service = null;
	
	public CommonQueryService() {
		commonQueryloader = new CommonQueryLoader();
		olapService = new OlapService();
	    dc_service = new DatacubeService(); 
	}
    
	/**
	 * return MDX statement of common query
	 * @param map
	 * @return
	 */
	public String getMDX(Map<String, String> map) {
		String cube_name = map.get("cube");
		Datacube datacube = dc_service.getDatacubeByName(cube_name);
		OlapConnection conn = olapService.getOlapConnByCube(datacube);
		Cube cube = null;
		try {
			cube = conn.getOlapSchema().getCubes().get(cube_name);
		} catch (OlapException e) {
			logger.error(e.getMessage());
		}
		Query query = null;
		try {
			query = new Query("Mondrian_query",cube);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return commonQueryloader.getMDX(query, map);	
	}
	
	/**
	 * return format :["Hangzhou","F","27","314.34","8487"]
	 * @param cube_name
	 * @param mdx
	 * @return
	 */
	public List<List<String>> simpleQuery(String cube_name, String mdx) {
		List<List<String>> result = new ArrayList<List<String>>();
		Datacube datacube = dc_service.getDatacubeByName(cube_name);
		OlapConnection conn = olapService.getOlapConnByCube(datacube);
		OlapLoader olapLoader = new OlapLoader();
		CellSet cs = null;
		try {
			cs = olapLoader.execute(mdx, conn);
		} catch (OlapException e) {
			logger.error(e.getMessage());
		}
		List<String> single_row = null;
		if(cs.getAxes().size()>1){
			for (Position row : cs.getAxes().get(1)) {		
			     single_row = new ArrayList<String>();
			     for (Member member : row.getMembers()) {	 
				     single_row.add(member.getName());
		         }
		         for (Position column : cs.getAxes().get(0)) {      
		        	 Cell cell = cs.getCell(column, row);
		             single_row.add(cell.getFormattedValue());	             
		         }
		         result.add(single_row);
		    }
		}else {
			for(Position column:cs.getAxes().get(0))
			{   
				single_row = new ArrayList<String>();
		        Cell cell = cs.getCell(column);
		        single_row.add(cell.getFormattedValue());	             
		        result.add(single_row);
			}
		}
		return result;
	}
}
