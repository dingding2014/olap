package com.netease.hz.model;

import org.olap4j.CellSet;

/**
 * Created by zhifei on 3/23/15.
 */
public class Query {
    private int state; //-1 fail 0 init 1 success
    private long startTime;
    private long endTime;
    private String result;
    private String query;


    public Query(long startTime, String query, int state) {
        this.startTime = startTime;
        this.query = query;
        this.state = state;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
