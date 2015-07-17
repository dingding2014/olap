package com.netease.hz.dao;

import com.netease.hz.model.Query;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by zhifei on 3/20/15.
 */
public class QueryLoader extends MySqlMetaLoader {
    private static final Logger logger = Logger.getLogger(QueryLoader.class);

    /**
     * Peresis a new query. state 0 for new, 1 for success and -1 for fail
     * @param query
     * @param startTime
     * @param state
     * @return
     */
    public long addQuery(String query, long startTime, int state) {
        BigInteger id = new BigInteger("-1");
        String sql = "insert into `query` ( `query`, `start_time`, `state`) values (?,?,?)";

        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;

        try {
            conn = this.getDBConnection(true);
            qr.update(conn, sql, query, startTime, state);
            id = (BigInteger) qr.query(conn, "SELECT LAST_INSERT_ID()", new ScalarHandler(1));

        } catch (IOException e) {
            logger.error(e.getMessage());

        } catch (SQLException e) {
            logger.error(e.getMessage());

        } finally {
            DbUtils.closeQuietly(conn);
        }
        return id.longValue();
    }

    /**
     * Update the query.
     * @param id
     * @param query
     * @return
     */
    public int updateQuery(long id, Query query) {
        String sql = "update `query` set `finish_time`=?, `state`=?, `result`=? where `id`=? ";

        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        int affectedRows = 0;

        try {
            conn = this.getDBConnection(true);
            affectedRows = qr.update(conn, sql, query.getEndTime(), query.getState(), query.getResult(),id);

        } catch (IOException e) {
            logger.error(e.getMessage());

        } catch (SQLException e) {
            logger.error(e.getMessage());

        } finally {
            DbUtils.closeQuietly(conn);
        }

        return affectedRows;
    }

    /**
     * Get the result by query id.
     * @param id
     * @return
     */
    public String getResultByQueryId(long id) {
        String sql = "select `result` from `query` where `id`=" + id;

        String result =  null;

        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;


        try {
            conn = this.getDBConnection(true);
            result = (String)qr.query(conn, sql, new ScalarHandler(1));

        } catch (IOException e) {
            logger.error(e.getMessage());

        } catch (SQLException e) {
            logger.error(e.getMessage());

        } finally {
            DbUtils.closeQuietly(conn);
        }

        return result;
    }

    /**
     * Get the whole query object by query id.
     * @param id
     * @return
     */
    public Query getQueryById(long id) {

        String sql = "select * from `query` where `id`=" + id;
        String result =  null;

        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        Map<String, Object> map = null;
        try {
            conn = this.getDBConnection(true);
            map = qr.query(conn, sql, new MapHandler());

        } catch (IOException e) {
            logger.error(e.getMessage());

        } catch (SQLException e) {
            logger.error(e.getMessage());

        } finally {
            DbUtils.closeQuietly(conn);
        }

        return this.getQueryFromMap(map);
    }


    private Query getQueryFromMap(Map<String, Object> map){
        if(map == null || map.size() == 0) {
            return null;
        }

        String query = (String)map.get("query");
        long startTime = (Long)map.get("start_time");
        int state = (Integer)map.get("state");

        Query q = new Query(startTime,query,state);

        Object endTime = map.get("finish_time");
        Object result = map.get("result");
        if(endTime != null){
            q.setEndTime((Long)endTime);
        }

        if(result != null) {
            q.setResult((String)result);
        }

        return q;
    }

}
