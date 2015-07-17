package com.netease.hz.dao;

import org.apache.commons.dbutils.QueryRunner;
import java.sql.Connection;

/**
 * Created by zhifei on 3/18/15.
 */
public interface BasicJdbcLoader {
    public Connection getDBConnection(boolean autoCommit) throws Exception;
    public QueryRunner createQueryRunner();
}
