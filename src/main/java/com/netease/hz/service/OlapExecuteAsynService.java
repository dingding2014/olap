package com.netease.hz.service;

import com.google.gson.Gson;
import com.netease.hz.dao.OlapLoader;
import com.netease.hz.dao.QueryLoader;
import com.netease.hz.model.DataSource;
import com.netease.hz.model.OlapCell;
import com.netease.hz.model.Query;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapException;
import org.olap4j.Position;
import org.olap4j.layout.CellSetFormatter;
import org.olap4j.layout.RectangularCellSetFormatter;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**The service which execute the mdx query in a asynchronous way.
 * When the query fails,
 * Created by zhifei on 3/25/15.
 */
public class OlapExecuteAsynService implements Runnable{
    private static final Logger logger = Logger.getLogger(OlapExecuteAsynService.class);
    private long querId;
    private Query query;
    private OlapConnection olapConnection;
    private OlapLoader olapLoader = null;
    private QueryLoader queryLoader = null;

    public OlapExecuteAsynService(long queryId, Query query, DataSource datasource, String xmlLocation) throws SQLException, ClassNotFoundException {
        this.querId = queryId;
        this.query = query;
        olapLoader = new OlapLoader();
        queryLoader = new QueryLoader();
        this.olapConnection = new OlapService().getOlapConnByDatasource(datasource, xmlLocation);
    }

    @Override
    public void run() {
        CellSet cs = null;
        try {
            cs = olapLoader.execute(query.getQuery(), olapConnection);
            /*
            //test search result
            System.out.println("查询结果如下: \n");
            CellSetFormatter formatter = new RectangularCellSetFormatter(false);
            formatter.format(cs, new PrintWriter(System.out, true));
            System.out.println("\n查询结果打印完毕");*/
        } catch (OlapException e) {       	
            logger.error("Execution error. QueryId: " + querId);
            query.setEndTime(System.currentTimeMillis());
            query.setResult("error:"+e.getMessage());
            query.setState(-1);
            updateQuery();
        } finally {
            DbUtils.closeQuietly(olapConnection);
        }
        
        List<Position> coordinates = new ArrayList<Position>();
        List<OlapCell> cellList = new ArrayList<OlapCell>();
        olapLoader.explore(cs.getAxes(), coordinates, cs, cellList);
        String result = new Gson().toJson(cellList);
        
        query.setEndTime(System.currentTimeMillis());
        query.setResult(result);
        query.setState(1);
        updateQuery();
        
        
    }

    private void  updateQuery() {
        queryLoader.updateQuery(querId, query);
    }
}
