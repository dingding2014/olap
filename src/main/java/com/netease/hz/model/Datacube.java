package com.netease.hz.model;


import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.Expose;


/**
 * Created by zhifei on 3/18/15.
 */
public class Datacube {
    @Expose
    private String name;
    @Expose
    private String datasource;
    @Expose
    private List<String> Dimensions_name;
    @Expose
    private List<String> Measures_name;
    
    private String schema;//Invisible for json convert
    
    public Datacube(String name, String datasource, String schema) {
        this.name = name;
        this.datasource = datasource;
        this.schema = schema;
    }

    public Datacube() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
      
    public List<String> getDimensions_name() {
    	return Dimensions_name;
    }
    
    public void setDimensions_name(List<String> name) {
    	//collect memory clearly(set null)
    	Dimensions_name = null;
    	Dimensions_name =  new ArrayList<String>();
    	this.Dimensions_name = name;
    }
    
    public List<String> getMeasure_name() {
    	return Measures_name;
    }
    
    public void setMeasures_name(List<String> name) {
    	//collect memory clearly(set null)
    	Measures_name = null;
    	Measures_name =  new ArrayList<String>();
    	this.Measures_name = name;
    }
    
}
