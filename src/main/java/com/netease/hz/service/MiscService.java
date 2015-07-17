package com.netease.hz.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.olap4j.OlapConnection;

import com.netease.hz.dao.DatacubeLoader;
import com.netease.hz.dao.DatasourceLoader;
import com.netease.hz.dao.DatasourceMetaLoader;
import com.netease.hz.dao.OlapLoader;
import com.netease.hz.dao.SchemaLoader;
import com.netease.hz.model.DataSource;
import com.netease.hz.model.Datacube;
import com.netease.hz.model.Schema;
import com.netease.hz.utils.Props;

/**
 * Created by zhuqinghua on 4/7/15.
 */
public class MiscService {
	
    private OlapLoader loader = null;

    public MiscService() {
        loader = new OlapLoader();
    }

    public List<String> getLevelMembers(String cube_name, String dimension_name, String level_name, Integer offset, Integer limit) {
    	DatacubeLoader dc_loader = new DatacubeLoader();
    	Datacube datacube = dc_loader.getDatacubeByName(cube_name);
    	OlapService olapservice = new OlapService();
    	OlapConnection conn = olapservice.getOlapConnByCube(datacube); 	
        return loader.getLevelMembers(conn, cube_name, dimension_name, level_name, offset, limit);
       
    }
    
    public int getLevelMemberCount(String cube_name, String dimension_name, String level_name) {
    	DatacubeLoader dc_loader = new DatacubeLoader();
    	Datacube datacube = dc_loader.getDatacubeByName(cube_name);
    	OlapService olapservice = new OlapService();
    	OlapConnection conn = olapservice.getOlapConnByCube(datacube); 	
        return loader.getLevelMemberCount(conn, cube_name, dimension_name, level_name);
        
    }
    
    public List<String> getMembersOfDimension(String cube_name, String dimension_name, Integer offset, Integer limit) {
    	DatacubeLoader dc_loader = new DatacubeLoader();
    	Datacube datacube = dc_loader.getDatacubeByName(cube_name);
    	OlapService olapservice = new OlapService();
    	OlapConnection conn = olapservice.getOlapConnByCube(datacube); 	
        return loader.getMembersOfDimension(conn, cube_name, dimension_name, offset, limit);
       
    }
    
    public int getMemberCountOfDimension(String cube_name, String dimension_name) {
    	DatacubeLoader dc_loader = new DatacubeLoader();
    	Datacube datacube = dc_loader.getDatacubeByName(cube_name);
    	OlapService olapservice = new OlapService();
    	OlapConnection conn = olapservice.getOlapConnByCube(datacube); 	
        return loader.getMemberCountOfDimension(conn, cube_name, dimension_name);
        
    }
   
    public List<List<String>> getMembersOfmultiDimensions(String cube_name, List<String> query_dimension) {
    	DatacubeService dc_service = new DatacubeService();
    	Datacube dc = dc_service.getDatacubeByName(cube_name);
    	SchemaLoader schema_loader = new SchemaLoader();
    	Schema schema = schema_loader.getSchema(dc.getSchema());
    	schema.getFileLocation();
    	String sql = null;
    	try {
			sql = schema_loader.praseXml(schema.getFileLocation(), dc.getName(), query_dimension);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    	DatasourceLoader ds_loader = new DatasourceLoader();
    	DataSource ds = ds_loader.getDatasourceByName(dc.getDatasource());
    	DatasourceMetaLoader dsMeta_loader = new DatasourceMetaLoader();
    	List<List<String>> result = dsMeta_loader.queryOfmultiDimension(ds, sql);
    	return result;
    }
}
