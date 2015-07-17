package com.netease.hz.model;

import java.util.List;

/**
 * this class is used for json data transform of common query and simpleQuery interface
 * Created by zhuqinghua on 4/13/15.
 */
public class QueryCondition {
	private String dimension;
	private String level = null;
	private List<String> members;
	private int axis;
	private String measure;
	private String method;
	private String value;
	private int operator;
	private int type;
	private String augument;
	private int connector;
    
	public QueryCondition() {
		//if connector==0, it means connector doesn't exist
		connector = 0;
	}
	
	public String getDimension() {
		return dimension;
	}
	
	public void setDimension(String Dimension) {
		this.dimension = Dimension;
	}
	
	public String getLevel() {
		return level;
	}
	
	public void setLevel(String Level) {
		this.level = Level;
	}
	
	public List<String> getMembers() {
		return members;
	}
	
	public void setMembers(List<String> Members) {
		this.members = Members;
	}
	
	public int getAxis() {
		return axis;
	}
	
	public void setAxis(int Axis_id) {
		this.axis = Axis_id;
	}
	
	public String getMeasure() {
		return measure;
	}
	
	public void setMeasure(String Measure) {
		this.measure = Measure;
	}
	
	public String getMethod() {
		return method;
	}
	
	public void setMethod(String Method) {
		this.method = Method;
	}
	
	public String getValue() {
		return value;
	}
	
	public void setValue(String Value) {
		this.value = Value;
	}
	
	public int getOperator() {
		return this.operator;
	}
	
	public int getType() {
		return this.type;
	}
	
	public String getAugument() {
		return this.augument;
	}
	
	public int getConnector() {
		return this.connector;
	}
}
