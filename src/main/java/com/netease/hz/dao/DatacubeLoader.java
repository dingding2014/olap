package com.netease.hz.dao;

import com.netease.hz.model.Datacube;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhifei on 3/20/15.
 */
public class DatacubeLoader extends MySqlMetaLoader {
    private static final Logger logger = Logger.getLogger(DatacubeLoader.class);

    public int addCube(String cubename, long schemaId, String datasource) throws Exception {
        String sql = "insert into `datacube` ( `name`, `schema_id`, `datasource_name`) values (?,?,?)";
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        int affectedRows = 0;
        try {
            conn = this.getDBConnection(true);
            affectedRows = qr.update(conn, sql, cubename, schemaId, datasource);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;

        } finally {
            DbUtils.closeQuietly(conn);
        }
        return affectedRows;
    }

    /**
     * 根据name， 删除datacube
     * @param datacubeName
     * @return
     */
    public int deleteDatacube(String datacubeName) {
        int affectedRows = 0;
        String sql = "delete from `datacube` where `name`=?";
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;

        try{
            conn = this.getDBConnection(true);
            affectedRows =  qr.update(conn, sql, datacubeName);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }

        return affectedRows;
    }

    public Datacube getDatacubeByName(String name) {
        String sql = "select `datacube`.`name`,`datacube`.`datasource_name`,`datacube`.`schema_id` from `datacube` where `datacube`.`name`=?";
        Datacube datacube = null;
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        try {
            conn = this.getDBConnection(true);
            Map<String, Object> map = qr.query(conn, sql, new MapHandler(), name);
            datacube = this.getDatacubeFromMap(map);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
           logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }

        return datacube;
    }

    public List<Datacube> getAllDatacubes() {
        List<Datacube> cubeList = new ArrayList<Datacube>();
        String sql = "select `datacube`.`name`,`datacube`.`datasource_name`,`datacube`.`schema_id` from `datacube`";
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        try {
            conn = this.getDBConnection(true);
            List<Map<String, Object>> maps = qr.query(conn, sql, new MapListHandler());
            for(Map<String, Object> map : maps) {
                Datacube cube = this.getDatacubeFromMap(map);
                if((!cube.getName().equals("")) && cube.getName() != null) {
                    cubeList.add(cube);
                }
            }

        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }

        return cubeList;
    }
    
    /**
     * get Cubes by DataSource name
     * @param name
     * @return
     */
    public List<Datacube> getCubesByDatasource(String name) {
    	List<Datacube> cubeList = new ArrayList<Datacube>();
    	String sql = "select `name`,`datasource_name` from datacube where `datasource_name`='"+name+"'";
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        try {
            conn = this.getDBConnection(true);
            List<Map<String, Object>> maps = qr.query(conn, sql, new MapListHandler());
            for(Map<String, Object> map : maps) {
                Datacube cube = this.getDatacubeFromMap(map);
                if((!cube.getName().equals("")) && cube.getName() != null) {
                    cubeList.add(cube);
                }
            }

        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }
    	
    	return cubeList;
    }

    private Datacube getDatacubeFromMap( Map<String, Object> map) {
        Datacube datacube = new Datacube();
        if(map.size() == 0) {
            logger.info("Can not load a datacube from query result");
            return datacube;
        }
        datacube.setName((String)map.get("name"));
        datacube.setSchema(String.valueOf(map.get("schema_id")));
        datacube.setDatasource((String) map.get("datasource_name"));

        return datacube;
    }
    
}
