package com.netease.hz.dao;

import com.netease.hz.utils.Props;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;

/**Mysql jdbc loader
 * Created by zhifei on 3/18/15.
 */
public class MySqlMetaLoader implements BasicJdbcLoader {
    private static final Logger logger = Logger.getLogger(MySqlMetaLoader.class);

    private BasicDataSource dataSource;

    public MySqlMetaLoader() {
    	
        Props p = Props.getInstance();
        HashMap<String, String> props = p.get_current();
        String port = props.get("mysql.port");
        String host = props.get("mysql.host");
        String database = props.get("mysql.database");
        String user = props.get("mysql.user");
        String password = props.get("mysql.password");
        String numConnections = props.get("mysql.numconnections");

        dataSource = new MysqlDataSource(host, port, database, user, password,
                numConnections);
    }

    @Override
    public Connection getDBConnection(boolean autoCommit) throws IOException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(autoCommit);
        } catch (Exception e) {
            DbUtils.closeQuietly(connection);
            logger.error("Getting Mysql connection failed!");
            throw new IOException("Error getting DB connection.", e);
        }
        return connection;
    }

    @Override
    public QueryRunner createQueryRunner() {
        return new QueryRunner(dataSource);
    }


   private static class MysqlDataSource extends BasicDataSource {

        private MysqlDataSource(String host, String port, String dbName,
                                String user, String password, String numConnections) {
            super();
            String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
            addConnectionProperty("useUnicode", "yes");
            addConnectionProperty("characterEncoding", "UTF-8");
            setDriverClassName("com.mysql.jdbc.Driver");
            setUsername(user);
            setPassword(password);
            setUrl(url);
            setMaxActive(Integer.valueOf(numConnections));
            setTestOnBorrow(true);
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }

       @Override
       public <T> T unwrap(Class<T> iface) throws SQLException {
           return null;
       }

       @Override
       public boolean isWrapperFor(Class<?> iface) throws SQLException {
           return false;
       }
   }


}


