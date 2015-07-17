package com.netease.hz.service;

import com.google.gson.Gson;
import com.netease.hz.dao.*;
import com.netease.hz.model.*;
import com.netease.hz.utils.Props;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhifei on 3/25/15.
 */
public class OlapService {

    private static final Logger logger = Logger.getLogger(OlapService.class);
    private OlapLoader olapLoader = null;
    private DatasourceLoader datasourceLoader = null;
    private QueryLoader queryLoader = null;
    private DatacubeLoader datacubeLoader = null;
    private SchemaLoader schemaLoader = null;


    public OlapService() {
        olapLoader = new OlapLoader();
        datasourceLoader = new DatasourceLoader();
        queryLoader = new QueryLoader();
        datacubeLoader = new DatacubeLoader();
        schemaLoader = new SchemaLoader();
    }


    public String executeSync(String cubeName, String mdx) {
        Datacube datacube = datacubeLoader.getDatacubeByName(cubeName);
        String xmlId = datacube.getSchema();
        String datasourceName = datacube.getDatasource();
        DataSource datasource = datasourceLoader.getDatasourceByName(datasourceName);
        Schema schema  = schemaLoader.getSchema(xmlId);
        return executeSyncHandler(datasource, schema.getFileLocation(), mdx);
    }

    public String executeSyncHandler(DataSource datasource, String xmlLocation, String mdx) {
        OlapConnection conn = null;
        try {
            conn = getOlapConnByDatasource(datasource, xmlLocation);
        } catch (SQLException  e) {
            logger.error(e.getMessage());
            return "";
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            return "";
        }

        CellSet cs = null;
        try {
            cs = olapLoader.execute(mdx, conn);
        } catch (OlapException e) {
            logger.error(e.getMessage());
            return "";
        }

        List<Position> coordinates = new ArrayList<Position>();
        List<OlapCell> cellList = new ArrayList<OlapCell>();
        olapLoader.explore(cs.getAxes(), coordinates, cs, cellList);
        String result = new Gson().toJson(cellList);

        return result;
    }



    public long executeAsyn(String cubeName, String mdx) {

        Datacube datacube = datacubeLoader.getDatacubeByName(cubeName);
        String xml = datacube.getSchema();
        String datasourceName = datacube.getDatasource();
        DataSource datasource = datasourceLoader.getDatasourceByName(datasourceName);
        Schema schema  = schemaLoader.getSchema(xml);
         
        return executeAsynHandler(datasource, schema.getFileLocation(), mdx);
    }

    public long executeAsynHandler(DataSource datasource, String xmlLocation, String mdx) {

        Query query = new Query(System.currentTimeMillis(), mdx, 0);
        long id = -1;

        try {
            id = queryLoader.addQuery(query.getQuery(), query.getStartTime(), query.getState());
            Thread t = new Thread(new OlapExecuteAsynService(id, query, datasource, xmlLocation));
            t.start();

        } catch (SQLException e) {
            logger.error(e.getMessage());
            return id;
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            return id;
        }

        return id;

    }

    public Query getQueryById(long id) {
        return queryLoader.getQueryById(id);
    }

    /**
     *
     * Get the datasource by its name. If the ds is hive, the Kerberos certification is needed.
     * @param datasource
     * @param xmlLocaiton
     * @return
     */
    public OlapConnection getOlapConnByDatasource(DataSource datasource, String xmlLocaiton) throws SQLException, ClassNotFoundException {


        if(datasource.getType().equalsIgnoreCase("hive")) {
            kerberosCertification();
        }
        String partUrl = datasource.getMondrianUrl();
        String entireMondrianUrl = partUrl + "Catalog='file:"+xmlLocaiton+"';";
        //entireMondrianUrl =  "jdbc:mondrian:Jdbc='jdbc:mysql://10.120.154.1:3306/test?user=olap&password=8Sl^U@Lqi&useUnicode=true&characterEncoding=utf8';Catalog='file:/Users/zhifei/Documents/foodmart.xml';JdbcDrivers=com.mysql.jdbc.Driver;";
        logger.info("Got the mondrian URL as " + entireMondrianUrl);

        return olapLoader.getConnection(entireMondrianUrl);

    }
    
    /**
     * return OlapConnection By Cube
     * @param datacube
     * @return
     */
    public OlapConnection getOlapConnByCube(Datacube datacube) {
    	DatasourceLoader ds_loader = new DatasourceLoader();
    	DataSource ds = ds_loader.getDatasourceByName(datacube.getDatasource());
    	SchemaLoader schema_loader = new SchemaLoader();
    	Schema schema = schema_loader.getSchema(datacube.getSchema());
    	OlapConnection conn= null;
		try {
			conn = this.getOlapConnByDatasource(ds, schema.getFileLocation());
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		} catch (SQLException e) {		
			logger.error(e.getMessage());
		}
    	return conn;
    }
    
    /**
     * This implementation is ugly!
     */
    public void kerberosCertification() {
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        Props p=Props.getInstance();
        HashMap<String, String> props = p.get_current();
        String keytabPrincipal = props.get("keytabPrincipal");
        String keytabName = props.get("keytabName");
        String keytabFilePath=ClassLoader.getSystemResource(keytabName).getPath();
        try {
            UserGroupInformation.loginUserFromKeytab(keytabPrincipal, keytabFilePath);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

}
