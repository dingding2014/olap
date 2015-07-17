package com.netease.hz.model;

import java.util.Properties;

/**
 * Created by zhifei on 3/18/15.
 */
public class DataSource {

    private String name;
    private String type; //mysql oracle hive
    private String host;
    private String port;
    private String database;
    private String username = "";
    private String password = "";
    private Properties para = new Properties();

    private String drivername;
    private String url;

    public DataSource(){}

    public DataSource(String name) {
        this.name = name;
    }

    public Properties getPara() {
        return para;
    }

    public void setPara(Properties para) {
        this.para = para;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }


    public String getMondrianUrl() {
        url = "jdbc:mondrian:";

        if (type.equalsIgnoreCase("mysql")) {
            url = url + "Jdbc='jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=utf8';"
                    + "JdbcDrivers=com.mysql.jdbc.Driver;";
        } else if (type.equalsIgnoreCase("hive")) {
            url = url + "Jdbc=jdbc:hive2://" + host + ":" + port + ";" + "JdbcUser=" + username + ";JdbcPassword=" + password + ";" +
                    "JdbcDrivers=org.apache.hive.jdbc.HiveDriver;";
        }
        return url;
    }

    public String getUrl() {
        if (type.equalsIgnoreCase("mysql")) {
            url = "jdbc:mysql://" + host + ":" + port + "/" + database;
        } else if (type.equalsIgnoreCase("hive")) {
            //url="jdbc:hive2://inspur116.photo.163.org:10000/test4;principal=hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM?mapred.job.queue.name=default";
            url = "jdbc:hive2://" + host + ":" + port + "/" + database + ";principal=hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM?mapred.job.queue.name=default";
        } else if (type.equalsIgnoreCase("oracle")) {
        	url = "jdbc:oracle:thin:@"+host+":"+port+":"+database; 
        }
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDrivername() {
        //This should also be recoded
        //http://hzw2312.blog.51cto.com/2590340/748307
        if (type.equalsIgnoreCase("mysql")) drivername = "com.mysql.jdbc.Driver";
        else if (type.equalsIgnoreCase("hive")) drivername = "org.apache.hive.jdbc.HiveDriver";
        else if (type.equalsIgnoreCase("oracle")) drivername = "oracle.jdbc.driver.OracleDriver";
        return drivername;
    }

    public void setDrivername(String drivername) {
        this.drivername = drivername;
    }
}
