package com.netease.hz.model;

/**
 * Created by zhifei on 3/18/15.
 */
public class Schema {
    private String name;
    private String fileLocation;

    public Schema(int id, String fileLocation){
        this.name = name;
        this.fileLocation = fileLocation;
    }

    public Schema() {}

    public Schema(String name) {
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }
}
