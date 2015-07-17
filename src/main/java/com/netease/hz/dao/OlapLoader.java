package com.netease.hz.dao;

import com.netease.hz.model.OlapCell;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.olap4j.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Measure;
import org.olap4j.metadata.Member;
import org.olap4j.query.Query;
import org.olap4j.query.QueryDimension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhifei on 3/20/15.
 */
public class OlapLoader {
    private static final Logger logger = Logger.getLogger(OlapLoader.class);

    /**
     * Get the OlapConnection from mondrian URL
     *
     * @param url
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public OlapConnection getConnection(String url) throws ClassNotFoundException, SQLException {
        Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
        Connection connection = DriverManager.getConnection(url);
        return connection.unwrap(OlapConnection.class);
    }

    /**
     * Execute the Olap query and return the cellset
     *
     * @param mdx
     * @param conn
     * @return
     * @throws OlapException
     */
    public CellSet execute(String mdx, OlapConnection conn) throws OlapException {
        OlapStatement statement = null;
        CellSet cellSet = null;
        try {
            statement = conn.createStatement();
            cellSet = statement.executeOlapQuery(mdx);
        } catch (OlapException e) {
            logger.error("Error when execute the mdx. Syntax error may occurred. query=" + mdx);
            throw new OlapException();
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(conn); 
        }
        return cellSet;
    }

    public void explore(List<CellSetAxis> axes, List<Position> coordinates, CellSet cs, List<OlapCell> cellList) {
        if(axes.size() == 0) {
            Cell cell = cs.getCell();
            OlapCell olapCell = new OlapCell(new Position[0], cell.getValue());
            cellList.add(olapCell);
            return;
        }
        exploreHandler(axes, coordinates, cs, cellList);
    }


    /**
     * Change the cellset to the json view. Coordinates.size should be 0 at very first.
     *
     * @param axes
     * @param coordinates
     * @param cs
     * @param cellList
     */
    public void exploreHandler(List<CellSetAxis> axes, List<Position> coordinates, CellSet cs, List<OlapCell> cellList) {
        int level = coordinates.size();
        if (level < axes.size()) {
            for (Position p : axes.get(level).getPositions()) {
                coordinates.add(p);
                exploreHandler(axes, coordinates, cs, cellList);
            }

            if (level > 0) {
                coordinates.remove(level - 1);
            }

        } else {
            Position[] positions = new Position[coordinates.size()];
            positions = coordinates.toArray(positions);
            Cell cell = cs.getCell(positions);
            OlapCell olapCell = new OlapCell(positions, cell.getValue());
            cellList.add(olapCell);
            coordinates.remove(level - 1);
        }
    }
    
    /**
     * return Dimensions name of cube
     * @param conn
     * @param cube_name
     * @return
     */
    public List<String> getDimensionsOfcube(OlapConnection conn,String cube_name) {
    	Cube cube = getCube(conn,cube_name);
		List<Dimension> Dimensions = new ArrayList<Dimension>();
		List<String> Dimensions_name = new ArrayList<String>();
		Dimensions = cube.getDimensions();
		for(Dimension d : Dimensions) {
			Dimensions_name.add(d.getName());
		}
		//0 Dimension is [Measure]
		return Dimensions_name.subList(1, Dimensions_name.size());	
    }
    
    /**
     * return Measures name of cube
     * @param conn
     * @param cube_name
     * @return
     */
    public List<String> getMeasuresOfcube(OlapConnection conn,String cube_name) {
    	Cube cube = getCube(conn,cube_name);
		List<Measure> Measures = new ArrayList<Measure>();
		List<String> Measures_name = new ArrayList<String>();
		Measures = cube.getMeasures();
		for(Measure m : Measures) {
			Measures_name.add(m.getName());
		}
		return Measures_name;  
    }
    
    /**
     * return certain amount of members of certain level of the cube
     * @param conn
     * @param cube_name
     * @param dimension_name
     * @param level_name
     * @param offset
     * @param limit
     * @return
     */
    public List<String> getLevelMembers(OlapConnection conn,String cube_name,String dimension_name,String level_name,
    		Integer offset,Integer limit) {
    	List<String> member_name = new ArrayList<String>();
    	Cube cube = getCube(conn,cube_name);
		Query query = createQuery(cube);
		Level certain_level = getLevel(query,dimension_name,level_name);
	    List<Member> members = new ArrayList<Member>();
	    members = getMembers(certain_level);
	    //if user didn't offer 'offset' parameter
	    if(offset==null) offset = 0;
	    //input check
	    if(offset>=members.size()) 
	    	return null;
        //if user didn't offer 'limit' parameter
	    if(limit==null) limit = members.size()-offset;
	    
	    if(offset+limit>members.size())
	    	limit = members.size()-offset;
	    members = members.subList(offset, offset+limit);
	    for(Member member: members) {
	    	member_name.add(member.getName());
		}
    	return member_name;
    }
    
    /**
     * return the count of members of certain level of the cube
     * @param conn
     * @param cube_name
     * @param dimension_name
     * @param level_name
     * @return
     */
    public int getLevelMemberCount(OlapConnection conn,String cube_name,String dimension_name,String level_name) {
    	Cube cube = getCube(conn,cube_name);
		Query query = createQuery(cube);
		Level certain_level = getLevel(query,dimension_name,level_name);
	    return getMembers(certain_level).size();
    }
    
    /**
     * return certain amount of members of certain Dimension of the cube
     * @param conn
     * @param cube_name
     * @param dimension_name
     * @param offset
     * @param limit
     * @return
     */
    public List<String> getMembersOfDimension(OlapConnection conn,String cube_name,String dimension_name,Integer offset,Integer limit) {
    	List<String> member_name = new ArrayList<String>();
    	Cube cube = getCube(conn,cube_name);
		Query query = createQuery(cube);
		QueryDimension dimension = query.getDimension(dimension_name);
		member_name = getMembers(dimension);
	    
	    //if user didn't offer 'offset' parameter
	    if(offset==null) offset = 0;
	    //input check
	    if(offset>=member_name.size()) 
	    	return null;
        //if user didn't offer 'limit' parameter
	    if(limit==null) limit = member_name.size()-offset;
	    
	    if(offset+limit>member_name.size())
	    	limit = member_name.size()-offset;
	    member_name = member_name.subList(offset, offset+limit);
	    
    	return member_name;
    }
    
    /**
     * return the count of members of certain Dimension of the cube
     * @param conn
     * @param cube_name
     * @param dimension_name
     * @return
     */
    public int getMemberCountOfDimension(OlapConnection conn,String cube_name,String dimension_name) {
    	List<String> member_name = new ArrayList<String>();
    	Cube cube = getCube(conn,cube_name);
		Query query = createQuery(cube);
		QueryDimension dimension = query.getDimension(dimension_name);
		member_name = getMembers(dimension);
	    return member_name.size();
    }
    
    public List<String> getMembers(QueryDimension dimension) {
    	List<String> member_name = new ArrayList<String>();
    	List<Hierarchy> hierarchy = dimension.getDimension().getHierarchies();
		List<Member> members = new ArrayList<Member>();
		for(Hierarchy h : hierarchy) {		
			List<Level> levels = h.getLevels();
			levels = levels.subList(1, levels.size());
			for(Level level : levels) {
				members = getMembers(level);
				for(Member member: members) {
	    	        member_name.add(member.getName());
		        }
			}		
		}
		return member_name;
    }
    
    /**
     * return Query of Olap4j
     * @param cube
     * @return
     */
    public Query createQuery(Cube cube) {
    	Query query = null;
		try {
			query = new Query("Mondrian_query",cube);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		return query;
    }
    
    /**
     * get cube by OlapConnection
     * @param conn
     * @param cube_name
     * @return
     */
    public Cube getCube(OlapConnection conn,String cube_name) {
    	Cube cube = null;
		try {
			cube = conn.getOlapSchema().getCubes().get(cube_name);
		} catch (OlapException e) {
			logger.error(e.getMessage());
		}
		return cube;
    }
    
    public Level getLevel(Query query,String dimension_name,String level_name) {
    	QueryDimension dimension = query.getDimension(dimension_name);
		List<Hierarchy> hierarchy = dimension.getDimension().getHierarchies();
		for(Hierarchy h : hierarchy) {		
			List<Level> levels = h.getLevels();
			for(Level level : levels) {
				if(level.getName().equalsIgnoreCase(level_name)) {
					return level;			
				}
			}
		}
		return null;
    }
    
    public List<Member> getMembers(Level level) {
    	List<Member> members = new ArrayList<Member>();
	    try {
			members = level.getMembers();
		} catch (OlapException e) {
			logger.error(e.getMessage());
		}
	    return members;
    }
}
