package com.netease.hz.utils;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by zhifei on 3/18/15.
 */
public class Props {
    private static final Logger log = Logger.getLogger(Props.class);
    private HashMap<String, String> _current = new HashMap<String, String>();
    private static Props instance = null;

    public static Props getInstance() {
        if (instance == null) {
            log.error("Null props instance");
            throw new NullPointerException("No props instance");
        }
        return instance;
    }

    public static void initInstance(String fileLocation) {
        instance = new Props(fileLocation);
    }

    private Props(String fileStr) {
        try {
            Properties p = getProperties(fileStr);
            for (String propName : p.stringPropertyNames()) {
                this._current.put(propName, p.getProperty(propName));
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public HashMap<String, String> get_current() {
        return _current;
    }

    /**
     * Tool method. Return a property object with a specific location
     * @param fileLocation
     * @return
     * @throws IOException
     */
    public static Properties getProperties(String fileLocation) throws IOException {
        FileInputStream input = new FileInputStream(fileLocation);
        Properties properties = new Properties();
        properties.load(input);
        input.close();
        return properties;
    }

    /**
     * Read a json and return corresponding properties object
     * @param json
     * @return
     */
    public static Properties getProperitesFromJson(String json) {
        Gson gson = new Gson();
        Properties p = new Properties();
        p = gson.fromJson(json, p.getClass());
        return p;
    }

    /**
     * Return a json string of the specific properties object
     * @param p
     * @return
     */
    public static String toJson(Properties p) {
        Gson gson = new Gson();
        return gson.toJson(p);
    }

}
