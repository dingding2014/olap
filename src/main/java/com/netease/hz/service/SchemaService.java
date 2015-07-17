package com.netease.hz.service;

import com.netease.hz.dao.SchemaLoader;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.*;

/**
 * Created by zhifei on 3/19/15.
 */
public class SchemaService {
    private static final Logger logger = Logger.getLogger(SchemaService.class);
    private SchemaLoader loader = null;

    public SchemaService() {
        loader = new SchemaLoader();
    }

    /***
     * @param filespath The absolute file path of the xml file.
     * @return
     */
    public long storeSchemas(String filespath) {
        return loader.addSchema(filespath);
    }

}
