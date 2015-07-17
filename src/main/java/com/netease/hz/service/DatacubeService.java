package com.netease.hz.service;

import com.netease.hz.dao.DatacubeLoader;
import com.netease.hz.dao.OlapLoader;


import com.netease.hz.model.DataSource;
import com.netease.hz.model.Datacube;

import org.apache.log4j.Logger;
import org.olap4j.OlapConnection;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by zhifei on 3/18/15.
 */

public class DatacubeService {
    private static final Logger logger = Logger.getLogger(DatacubeService.class);
    private DatacubeLoader loader = null;

    public DatacubeService() {
        loader = new DatacubeLoader();
    }

    /**
     *
     * @param filePath
     * @param datasource
     */
    public void addDatacube(String filePath, String datasource, long id) throws Exception {
        //存入所有xml文件，得到ID值
        OlapService olapService = new OlapService();
        DataSourceService dataSourceService = new DataSourceService();
        List<String> cubeList = new ArrayList<String>();

        DataSource ds = dataSourceService.getDataSourceByName(datasource);
        OlapConnection conn = olapService.getOlapConnByDatasource(ds, filePath);

        for(org.olap4j.metadata.Cube c : conn.getOlapSchema().getCubes()) {
            cubeList.add(c.getName());
        }

        for(String name : cubeList) {
            loader.addCube(name, id, datasource);
            logger.info("Insert datacubeName: " + name + ", fileId=:" + id + ", datasource: " + datasource);
        }

    }

    /**
     * @param name
     */
    public boolean deleteDatacube(String name) {

        if(loader.deleteDatacube(name) > 0) {
            return true;
        }
        return false;
    }

    public Datacube getDatacubeByName(String name) {
        return loader.getDatacubeByName(name);
    }

    public List<Datacube> getAllDatacubes() {
        return loader.getAllDatacubes();
    }
    
    /**
     * return the DataCube contain the information of Dimensions and Measures
     * @param name
     * @return
     */
    public Datacube getDatacubeModelByName(String name){
    	Datacube datacube =  loader.getDatacubeByName(name);
    	OlapService olapservice = new OlapService();
    	OlapConnection conn =  olapservice.getOlapConnByCube(datacube);
		OlapLoader olaploader = new OlapLoader();	
		List<String> Dimensions_name = olaploader.getDimensionsOfcube(conn, name);
		List<String> Measures_name = olaploader.getMeasuresOfcube(conn, name);
		datacube.setDimensions_name(Dimensions_name);
		datacube.setMeasures_name(Measures_name);
		return datacube;
    }
    
    public List<Datacube> getCubesByDatasource(String name) {
    	return loader.getCubesByDatasource(name);
    }
    
}
