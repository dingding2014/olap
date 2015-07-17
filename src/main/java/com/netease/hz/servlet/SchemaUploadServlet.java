package com.netease.hz.servlet;

import com.google.gson.Gson;
import com.netease.hz.service.DatacubeService;
import com.netease.hz.service.SchemaService;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.*;

/**
 * File uploading servlet. When upload a schema file.
 * The data cubes in this file are also added.
 * Created by zhifei on 3/19/15.
 */
public class SchemaUploadServlet extends HttpServlet {
    private static final Logger logger = Logger.getLogger(SchemaUploadServlet.class);


    private boolean isMultipart;
    private String filePath;
    private int maxFileSize = 5 * 1024 * 1024;
    private File file;


    public void init() {
        ServletContext context = getServletContext();
        String prefix;
        URL resource;
        try {
            resource = this.getClass().getClassLoader().getResource("/");
            if (resource == null) {
                logger.error("Resources directory not found!");
                throw new NullPointerException("Cannot get resources directory from context");
            }
            prefix = resource.getPath();

        } catch (NullPointerException e) {
            logger.error(e.getMessage());
            return;
        }
        filePath = prefix + context.getInitParameter("file-upload");
        File f = new File(filePath);
        if (!f.exists()) {
            f.mkdir();
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        return;
    }

    /**
     * Upload the schema file. If sucess, return 0; Otherwise, 1;
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	//solve the problem of cross-domain
    	response.setHeader("Access-Control-Allow-Origin", "*");
    	response.setContentType("text/json"); 
		response.setCharacterEncoding("UTF-8"); 
    	
    	isMultipart = ServletFileUpload.isMultipartContent(request);
        Map<String, String> map = new HashMap<String, String>();
        Gson gson = new Gson();
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();

        if (!isMultipart) {
            map.put("error", "Post request is not multipart");
            out.write(gson.toJson(map));
            return;
        }

        long id = 0;
        try {
            id = handleUpload(request);
        } catch (Exception e) {
            map.put("error", "Error occured when upload file" + e.getMessage());
            logger.error(e.getMessage());
            out.write(gson.toJson(map));
            return;
        }
        map.put("fileId", String.valueOf(id));
        out.write(gson.toJson(map));
    }

    /**
     * Handle file uploading task
     *
     * @param request
     * @return
     */
    private long handleUpload(HttpServletRequest request) throws Exception {
        String absolutePath = "";
        String datasource = null;
        DiskFileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);

        upload.setSizeMax(maxFileSize);

        try {
            List fileItems = upload.parseRequest(request);
            Iterator i = fileItems.iterator();

            while (i.hasNext()) {
                FileItem item = (FileItem) i.next();
                if (!item.isFormField()) {
                    String fileName = item.getName();
                    if (!fileName.endsWith("xml")) {
                        logger.info("Uploading error: the file " + fileName + " is not xml file.");
                        return -1;
                    }
                    if (fileName.lastIndexOf("\\") >= 0) {
                        file = new File(filePath,
                                fileName.substring(fileName.lastIndexOf("\\")));
                    } else {
                        file = new File(filePath,
                                fileName.substring(fileName.lastIndexOf("\\") + 1));
                    }
                    item.write(file);
                    absolutePath = file.getAbsolutePath();
                    logger.info("File " + fileName + "has been uploaded. Path=" + file.getAbsolutePath());
                } else if (item.getFieldName().equals("datasource")) {
                    datasource = item.getString();
                    logger.info("datasource=" + datasource);
                }
            }
        } catch (Exception ex) {
            logger.error("File uploading failed " + ex.getMessage(), ex);
        }

        if (absolutePath.equals("") || absolutePath == null || datasource.equals("") || datasource == null) {
            return -1;
        }

        DatacubeService datacubeService = new DatacubeService();
        SchemaService xmlService = new SchemaService();
        //Get the id of schemas in database.
        long fileId = xmlService.storeSchemas(absolutePath);
        datacubeService.addDatacube(absolutePath, datasource, fileId);
        return fileId;
    }
}
