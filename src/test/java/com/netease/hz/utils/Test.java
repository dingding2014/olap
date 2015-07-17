package com.netease.hz.utils;

import com.google.gson.Gson;
import mondrian.rolap.aggmatcher.DefaultDef;
import org.olap4j.*;
import org.olap4j.metadata.Member;
import sun.misc.BASE64Encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by zhifei on 3/18/15.
 */
public class Test {
    public static int count = 0;

    public static String mdx = "SELECT\n" +
            "{[Product].[All Products].[Drink].[Beverages].Children} ON COLUMNS\n" +
            "FROM [Sales]\n" +
            "WHERE ([Time].[1997])";

    public static String url ="jdbc:mondrian:"
                    //连接数据源的JDBC连接
                    + "Jdbc='jdbc:mysql://10.214.224.214:3306/foodmart?user=root&password=root&useUnicode=true&characterEncoding=utf8';"
                    //数据模型文件
                    + "Catalog='file:/Users/zhifei/Documents/foodmart.xml';"
                    //连接数据源用到的驱动
                    + "JdbcDrivers=com.mysql.jdbc.Driver;";


    public static OlapConnection getConnection(String url) throws ClassNotFoundException, SQLException {
        Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
        Connection connection = DriverManager.getConnection(url);
        return connection.unwrap(OlapConnection.class);
    }


    public static CellSet getResultSet(String mdx,OlapConnection conn) throws OlapException {
        OlapStatement statement = conn.createStatement();
        CellSet cellSet = statement.executeOlapQuery(mdx);
        return cellSet;
    }

    public static void main1(String[] args) throws ClassNotFoundException, SQLException {
        OlapConnection conn= getConnection(url);
        CellSet cs = getResultSet(mdx, conn);
        //CellSetAxis c;

        int count = 0;
        if(cs.getAxes().size()>1){
            for (Position row : cs.getAxes().get(1)) {
                for (Position column : cs.getAxes().get(0)) {
                    for (Member member : row.getMembers()) {
                        System.out.println("rows:"+member.getUniqueName());
                    }
                    for (Member member : column.getMembers()) {
                        System.out.println("columns:"+member.getUniqueName());
                    }
                    final Cell cell = cs.getCell(column, row);


                    System.out.println("values:"+cell.getValue());

                    Position[] positions = new Position[2];
                    positions[0] = column;
                    positions[1] = row;

                    OlapCell oalpCell = new OlapCell(positions, cell.getValue());

                    System.out.println("****" + oalpCell.toString());
                    System.out.println(count++);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        OlapConnection conn= getConnection(url);
        CellSet cs = getResultSet(mdx, conn);
        List<Position> coordinates = new ArrayList<Position>();
       List<OlapCell> results = new ArrayList<OlapCell>();
        explore(cs.getAxes(), coordinates, cs, results);
        Gson gson = new Gson();
        System.out.println(gson.toJson(results));

    }


    //coordinates.size should be 0 at very first
    public static void explore(List<CellSetAxis> axes, List<Position> coordinates, CellSet cs, List<OlapCell> cellList) {
        int level = coordinates.size();
        //System.out.println(level + "  " + axes.size());
        if(level < axes.size()) {
            for(Position p : axes.get(level).getPositions()) {
                coordinates.add(p);
                explore(axes,coordinates,cs, cellList);
            }

            if(level > 0) {
                coordinates.remove(level-1);
            }

        } else {
            Position[] positions = new Position[coordinates.size()];
            positions = coordinates.toArray(positions);
            Cell cell = cs.getCell(positions);
            OlapCell olapCell = new OlapCell(positions, cell.getValue());
            cellList.add(olapCell);
            //System.out.println((++count) + " " + olapCell.toString());
            coordinates.remove(level-1);
        }

    }
}

class OlapCell {
    private Map<String, Set<String>> coordinate = new HashMap<String, Set<String>>();
    private Object v;

    public OlapCell(Position[] positions, Object value) {
        v = value;
        for(int i=0; i<positions.length; i++) {
            Set<String> set = new HashSet<String>();

            for(Member member : positions[i].getMembers()) {
                set.add(member.getUniqueName());
            }

            coordinate.put(String.valueOf(i),set);

        }

    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }


}
