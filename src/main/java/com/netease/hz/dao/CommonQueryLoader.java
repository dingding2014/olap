package com.netease.hz.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.olap4j.Axis;
import org.olap4j.OlapException;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.query.Query;
import org.olap4j.query.QueryAxis;
import org.olap4j.query.QueryDimension;
import org.olap4j.query.SortOrder;
import org.olap4j.query.LimitFunction;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.netease.hz.model.QueryCondition;

/**
 * Created by zhuqinghua on 4/13/15.
 */
public class CommonQueryLoader {
	private static final Logger logger = Logger.getLogger(CommonQueryLoader.class);
	
	private List<Axis.Standard> Axis_names = null;		
	
	public CommonQueryLoader() {
		Axis_names = new ArrayList<Axis.Standard>();
		Axis_names.add(Axis.COLUMNS);
		Axis_names.add(Axis.ROWS);
		Axis_names.add(Axis.PAGES);
		Axis_names.add(Axis.CHAPTERS);
		Axis_names.add(Axis.SECTIONS);
	}
	
	/**
	 * return the MDX statement of common query
	 * @param query
	 * @param map
	 * @return
	 */
	public String getMDX(Query query, Map<String, String> map) {
		Gson gson = new Gson();
		List<QueryCondition> List_condition = new ArrayList<QueryCondition>();
		QueryCondition query_condition = null;
		
		String columns = map.get("columns");
		List_condition = gson.fromJson(columns, new TypeToken<List<QueryCondition>>(){}.getType());  
		this.setQueryAxes(query, Axis.COLUMNS, List_condition);	
		String rows = map.get("rows");
		List_condition = gson.fromJson(rows, new TypeToken<List<QueryCondition>>(){}.getType());  
		this.setQueryAxes(query, Axis.ROWS, List_condition);
		String where =  map.get("where");
		if(where!=null) {
			List_condition = gson.fromJson(where, new TypeToken<List<QueryCondition>>(){}.getType());  
			this.setQueryWhere(query, List_condition);
		}
		String OrderBy =  map.get("orderby");
		if(OrderBy!=null) {
			query_condition = gson.fromJson(OrderBy, new TypeToken<QueryCondition>(){}.getType());  
			this.setQueryOrderBy(query, query_condition);
		}
		String filter =  map.get("filter");
		if(filter!=null) {
			List_condition = gson.fromJson(filter, new TypeToken<List<QueryCondition>>(){}.getType());  
			this.setQueryfilter(query, List_condition);
		}
		String limit =  map.get("limit");
		if(limit!=null) {
			query_condition = gson.fromJson(limit, new TypeToken<QueryCondition>(){}.getType());  
			this.setQuerylimit(query, query_condition);
		}
		//return MDX string
		return query.getSelect().toString();
	}
    
	/**
	 * set query Axes information
	 * @param query
	 * @param Axis_name
	 * @param query_axes
	 */
	public void setQueryAxes(Query query, Axis Axis_name, List<QueryCondition> query_axes) {
		for(QueryCondition queryCondition : query_axes) {	
			QueryDimension dimension = query.getDimension(queryCondition.getDimension());		
			String level = queryCondition.getLevel();
			List<String> members = queryCondition.getMembers();	
			for(String member : members) {			
				includeMember(dimension, level, member);
			}	
			query.getAxis(Axis_name).addDimension(dimension);
		}
	}
	
	/**
	 * set query where condition
	 * @param query
	 * @param query_where
	 */
	public void setQueryWhere(Query query, List<QueryCondition> query_where) {
		for(QueryCondition queryCondition : query_where) {
			QueryDimension dimension = query.getDimension(queryCondition.getDimension());
			String level = queryCondition.getLevel();
			List<String> members = queryCondition.getMembers();
			for(String member : members) {
				includeMember(dimension, level, member);
			}	
			//'where' condition is at axis(-1) named FILTER
			query.getAxis(Axis.FILTER).addDimension(dimension);
		}
	}
	
	/**
	 * set query order information
	 * @param query
	 * @param query_OrderBy
	 */
	public void setQueryOrderBy(Query query, QueryCondition query_OrderBy) {
		int Axis_id = query_OrderBy.getAxis();
		String measure = "[Measures].["+query_OrderBy.getMeasure()+"]";
		String method = query_OrderBy.getMethod();
		sortAxis(query.getAxis(Axis_names.get(Axis_id)), measure, method);
	}
	
	/**
	 * set query filter information
	 * @param query
	 * @param query_filter
	 */
	public void setQueryfilter(Query query, List<QueryCondition> query_filter) {
		int Axis_id =  query_filter.get(0).getAxis();
		String condition = "";
		for(QueryCondition queryCondition : query_filter) {
			String measure = "[Measures].["+queryCondition.getMeasure()+"]";
			condition = condition + measure;
			int operator =  queryCondition.getOperator();
			if(operator==0) {
				condition += ">";
			}else if(operator==1) {
				condition += "<";
			}else {
				condition += "=";
			}
			int type = queryCondition.getType();
			String augument =  queryCondition.getAugument();
			if(type==0) {
				condition += augument;
			}else {
				condition = condition + "[Measures].[" +augument +"]";
			}
			int connector = queryCondition.getConnector();
			//if connector==0, it means connector doesn't exist
			if(connector==1) {
				condition += " and "; 
			}else if(connector==2) {
				condition += " or "; 
			}
		}
		filterAxis(query.getAxis(Axis_names.get(Axis_id)),condition);
	}
	
	/**
	 * set query limit information
	 * @param query
	 * @param query_limit
	 */
    public void setQuerylimit(Query query, QueryCondition query_limit) {
    	int Axis_id = query_limit.getAxis();
    	String measure = "[Measures].["+query_limit.getMeasure()+"]";
		String method = query_limit.getMethod();
		String value = query_limit.getValue();
		limitAxis(query.getAxis(Axis_names.get(Axis_id)), method, value, measure);
	}
	
	private void filterAxis(QueryAxis axis, String filterStr){
		axis.filter(filterStr);
	}

	private void sortAxis(QueryAxis axis, String sortStr, String sortOrder){
		SortOrder so = SortOrder.valueOf(sortOrder);
		axis.sort(so, sortStr);
	}
	
	private void limitAxis(QueryAxis axis, String limitFunction, String n, String sortLiteral){
		LimitFunction lf = LimitFunction.valueOf(limitFunction);
		BigDecimal bn = new BigDecimal(n);
		axis.limit(lf, bn, sortLiteral);
	}
	
	/**
	 * Specify query members
	 * @param dimension
	 * @param level_name
	 * @param memberName
	 */
	private void includeMember(QueryDimension dimension, String level_name, String memberName) {
		if(dimension.getName().equalsIgnoreCase("Measures"))
			level_name = "MeasuresLevel";
		for (Hierarchy hierarchy : dimension.getDimension().getHierarchies()) {	
				for (Level level : hierarchy.getLevels()) {	
					if(level_name==null||level.getName().equalsIgnoreCase(level_name)) {				
						try {
							for(Member mem : level.getMembers()) {
								if(mem.getName().endsWith(memberName))
									dimension.include(mem);							
							}
						} catch (OlapException e) {
							logger.error(e.getMessage());
						}					
					}		
				}			
		}
	}
}
