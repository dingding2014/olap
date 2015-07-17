package com.netease.hz.service;


import com.netease.hz.dao.DatasourceLoader;
import com.netease.hz.dao.DatasourceMetaLoader;
import com.netease.hz.model.DataSource;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by zhuqinghua on 3/18/15.
 */
public class DataSourceService {
	private static final Logger logger = Logger
			.getLogger(DataSourceService.class);
	private DatasourceLoader loader = null;
	int affectedRows = 0;

	public DataSourceService() {
		loader = new DatasourceLoader();
	}

	/**
	 * Add a new data source
	 *
	 * @param ds
	 * @return
	 */
	public boolean addDataSource(DataSource ds) {
		affectedRows = loader.addDataSource(ds);
		if (affectedRows > 0)
			return true;
		else
			return false;
	}

	/**
	 * @param sourceName
	 * @return
	 */
	public boolean deleteDataSource(String sourceName) {

		affectedRows = loader.delete(sourceName);
		if (affectedRows > 0)
			return true;
		else
			return false;
	}

	/**
	 * modify DataSource
	 *
	 * @param DataSourceName
	 * @param field_name
	 * @param update_info
	 * @return
	 */
	public boolean modifyDataSource(DataSource ds) {

		affectedRows = loader.updateDatasource(ds);
		if (affectedRows > 0)
			return true;
		else
			return false;
	}

	/**
	 * get certain DataSource By name
	 *
	 * @param sourceName
	 * @return
	 */
	public DataSource getDataSourceByName(String sourceName) {
		return loader.getDatasourceByName(sourceName);
	}

	/**
	 * get all DataSource
	 *
	 * @return
	 */
	public Map<String,List<String>> getAllDataSource(String pattern) {
		// Comments here
		// loader
		return loader.getAllDatasources(pattern);
	}

	/**
	 * get all avaliable table by certain DataSource
	 *
	 * @param ds_name
	 * @return
	 * @throws SQLException
	 */
	public Map<String,List<String>> getTableofDatasource(String ds_name) {
		// get DataSource
		DataSource tmp_ds = loader.getDatasourceByName(ds_name);
		// get table
		DatasourceMetaLoader ds_loader = new DatasourceMetaLoader();
		return ds_loader.getTableofDatasource(tmp_ds);
	}

	/**
	 * get Fields information of certain table
	 *
	 * @param ds_name
	 * @param table_name
	 * @return
	 */
	public Map<String,List<Map<String,String>>> getFieldsofTable(String ds_name, String table_name) {
		// get DataSource
		DataSource tmp_ds = loader.getDatasourceByName(ds_name);
		// get table
		DatasourceMetaLoader ds_loader = new DatasourceMetaLoader();
		return ds_loader.getFieldsofTable(tmp_ds, table_name);
	}
}
