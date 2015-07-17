package com.netease.hz.dao;

import com.netease.hz.model.DataSource;
import com.netease.hz.utils.Props;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhuqinghua on 3/20/15.
 */

public class DatasourceLoader extends MySqlMetaLoader {
	private static final Logger logger = Logger
			.getLogger(DatasourceLoader.class);

	/**
	 * Add a data source
	 *
	 * @param ds
	 * @return
	 */
	public int addDataSource(DataSource ds) {
		String sql = "insert into `datasource` (`name`,`type`,`host`,`port`,`database`,`username`,`passwd`,`drivename`,`properties`)"
				+ " values (?,?,?,?,?,?,?,?,?)";
		int affectedRows = 0;
		Connection conn = null;
		QueryRunner qr = this.createQueryRunner();
		try {
			conn = this.getDBConnection(true);
			affectedRows = qr.update(conn, sql, ds.getName(),ds.getType(),ds.getHost(),ds.getPort(),ds.getDatabase(),ds.getUsername(),ds.getPassword(), 
					ds.getDrivername(),Props.toJson(ds.getPara()));

		} catch (IOException e) {
			logger.error(e.getMessage());

		} catch (SQLException e) {
			logger.error(e.getMessage());

		} finally {
			DbUtils.closeQuietly(conn);
			return affectedRows;
		}

	}

	/**
	 * Delete operation
	 *
	 * @param sourceName
	 * @return
	 */
	public int delete(String sourceName) {
		String sql = "delete from datasource where name='" + sourceName + "'";
		int affectedRows = 0;
		Connection conn = null;
		QueryRunner qr = this.createQueryRunner();
		try {
			conn = this.getDBConnection(true);
			affectedRows = qr.update(conn, sql);

		} catch (IOException e) {
			logger.error(e.getMessage());

		} catch (SQLException e) {
			logger.error(e.getMessage());

		} finally {
			DbUtils.closeQuietly(conn);
			return affectedRows;
		}
	}

	/**
	 * Update a data source info by its name
	 * 
	 * @param ds
	 * @return
	 */
	public int updateDatasource(DataSource ds) {
		String sql = "update `datasource` set `name`=?,`type`=?,`host`=?,`port`=?,`database`=?,`username`=?,`passwd`=?,`drivename`=?,`properties`=? where `name`=? ";

		int affectedRows = 0;
		Connection conn = null;
		QueryRunner qr = this.createQueryRunner();
		try {
			conn = this.getDBConnection(true);
			affectedRows = qr.update(conn, sql, ds.getName(),ds.getType(),ds.getHost(),ds.getPort(),ds.getDatabase(),ds.getUsername(),ds.getPassword(), 
					ds.getDrivername(),
					 Props.toJson(ds.getPara()),ds.getName());

		} catch (IOException e) {
			logger.error(e.getMessage());

		} catch (SQLException e) {
			logger.error(e.getMessage());

		} finally {
			DbUtils.closeQuietly(conn);
			return affectedRows;
		}
	}

	/**
	 * Get a data source by its name
	 * 
	 * @param datasourceName
	 * @return
	 */
	public DataSource getDatasourceByName(String datasourceName) {
		String sql = "select `name`,`type`,`host`,`port`,`database`,`username`,`passwd`,`drivename`,`properties` from `datasource` where `name`='"
				+ datasourceName + "'";
		QueryRunner qr = this.createQueryRunner();
		Connection conn = null;
		DataSource ds = null;
		try {
			conn = this.getDBConnection(true);
			Map<String, Object> map = qr.query(conn, sql, new MapHandler());
			map.put("properties", "");
			ds = this.getDatasourceFromMap(map);

		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			DbUtils.closeQuietly(conn);
		}

		return ds;
	}
    
	/**
	 * get all DataSource
	 * @return
	 */
	public Map<String,List<String>>getAllDatasources(String pattern) {
		String sql;
		if (pattern == null || pattern.equalsIgnoreCase("")) {
			sql = "select `name`,`type`,`host`,`port`,`database`,`username`,`passwd`,`drivename`, `properties` from `datasource`";
		} else {
			sql = "select `name`,`type`,`host`,`port`,`database`,`username`,`passwd`,`drivename`, `properties` from `datasource`  where `name` like '%"
					+ pattern + "%'";
		}
		List<String> list = new ArrayList<String>();
		QueryRunner qr = this.createQueryRunner();
		Connection conn = null;
		try {
			conn = this.getDBConnection(true);
			List<Map<String, Object>> maps = qr.query(conn, sql,
					new MapListHandler());
			for (Map<String, Object> map : maps) {
					list.add(map.get("name").toString());
				}
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			DbUtils.closeQuietly(conn);
		}
		Map<String,List<String>> map = new HashMap<String,List<String>>();
		map.put("datasources", list);
		return map;
	}

	/**
	 *
	 * @param map
	 * @return
	 */
	public DataSource getDatasourceFromMap(Map<String, Object> map) {
		if (map.size() == 0) {
			return null;
		}
		String name = (String) map.get("name");
		String type = (String) map.get("type");
		String host = (String) map.get("host");
		String port = (String) map.get("port");
		String database = (String) map.get("database");
		String username = (String) map.get("username");
		String passwd = (String) map.get("passwd");
		String para = null;
		if(map.get("properties")!=null)
			para = map.get("properties").toString();
		DataSource ds = new DataSource(name);
		ds.setType(type);
		ds.setHost(host);
		ds.setPort(port);
		ds.setDatabase(database);
		ds.setUsername(username);
		ds.setPassword(passwd);
		if(para!=null)
			ds.setPara(Props.getProperitesFromJson(para));
		else ds.setPara(null);
		return ds;
	}
	
	
}
