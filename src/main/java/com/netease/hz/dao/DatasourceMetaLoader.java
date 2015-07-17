package com.netease.hz.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netease.hz.model.DataSource;
import com.netease.hz.service.DataSourceService;
import com.netease.hz.utils.Props;

import org.apache.commons.dbutils.DbUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * Created by zhuqinghua on 3/25/15.
 */

public class DatasourceMetaLoader {
	private static final Logger logger = Logger
			.getLogger(DataSourceService.class);

	/**
	 * 
	 * @param ds
	 * @return
	 */
	public Connection getDBConnection(DataSource ds) throws Exception {

		Connection conn = null;
		String url;
		Class.forName(ds.getDrivername());
		if (ds.getType().equalsIgnoreCase("mysql")) {
			// mysql
			url = ds.getUrl();
			String user = ds.getUsername();
			String password = ds.getPassword();
			conn = DriverManager.getConnection(url, user, password);
		} else if (ds.getType().equalsIgnoreCase("hive")) {
			// hive
			// kerberos
			Configuration conf = new Configuration();
			conf.setBoolean("hadoop.security.authorization", true);
			conf.set("hadoop.security.authentication", "kerberos");
			UserGroupInformation.setConfiguration(conf);
			Props p = Props.getInstance();
			HashMap<String, String> props = p.get_current();
			String keytabPrincipal = props.get("keytabPrincipal");
			String keytabName = props.get("keytabName");
			String keytabFilePath = ClassLoader.getSystemResource(keytabName)
					.getPath();
			UserGroupInformation.loginUserFromKeytab(keytabPrincipal,
					keytabFilePath);
			url = ds.getUrl();
			conn = DriverManager.getConnection(url);

		} else if (ds.getType().equalsIgnoreCase("oracle")){
			// oracles
			url = ds.getUrl();
			String user = ds.getUsername();
			String password = ds.getPassword();
			conn = DriverManager.getConnection(url,user,password);		
		}
		return conn;

	}

	/**
	 * get table
	 * 
	 * @param tmp_ds
	 * @return
	 */
	public Map<String,List<String>> getTableofDatasource(DataSource tmp_ds) {

		Connection conn = null;
		Statement stmt = null;
		ResultSet res = null;
		List<String> table_name = new ArrayList<String>();
		try {
			conn = getDBConnection(tmp_ds);
			stmt = conn.createStatement();
			res = stmt.executeQuery("show tables");
			while (res.next()) {
				table_name.add(res.getString(1));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			DbUtils.closeQuietly(res);
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(conn);
		}
		Map<String,List<String>> map = new HashMap<String,List<String>>();
		map.put("tables", table_name);
		return map;
	}

	/**
	 * get fields information
	 * 
	 * @param tmp_ds
	 * @param table_name
	 * @return
	 */
	public Map<String,List<Map<String,String>>> getFieldsofTable(DataSource tmp_ds,String table_name) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet res = null;
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		
		try {
			conn = getDBConnection(tmp_ds);
			stmt = conn.createStatement();
			res = stmt.executeQuery("describe " + table_name);
			while (res.next()) {
				Map<String, String> map = new HashMap<String, String>();
				map.put("name", res.getString(1).toString());
				int flag = res.getString(2).toString().indexOf("(");
				String type = res.getString(2).toString().substring(0, flag);
				map.put("type", type);
				list.add(map);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			DbUtils.closeQuietly(res);
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(conn);
		}
		Map<String,List<Map<String,String>>> map = new HashMap<String,List<Map<String,String>>>();
		map.put("fields", list);
		return map;
	}

	public List<List<String>> queryOfmultiDimension(DataSource tmp_ds, String sql) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet res = null;
		List<List<String>> result = new ArrayList<List<String>>();
		try {
			conn = getDBConnection(tmp_ds);
			stmt = conn.createStatement();
			res = stmt.executeQuery(sql);
		    ResultSetMetaData rsmd = null;
            rsmd = res.getMetaData();
            int field_count = rsmd.getColumnCount();
            List<String> single_row = null;
			while (res.next()) {
				single_row = new ArrayList<String>();
				for(int i=1;i<=field_count;i++) {
					single_row.add(res.getString(i));
				}
				result.add(single_row);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			DbUtils.closeQuietly(res);
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(conn);
		}
		return result;
	}
}
