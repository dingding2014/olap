package com.netease.hz.listener;

/**
 * Created by zhifei on 3/18/15.
 *
 * When server start, this listener will load the properties file
 */

import com.netease.hz.utils.Props;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;
import java.net.URL;

public class ConfigListener implements ServletContextListener {
    private static final Logger logger = Logger.getLogger(ConfigListener.class);

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        if (logger.isDebugEnabled()) {
            logger.info("Destroy LogListener....");
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent contextEvent) {
        ServletContext context = contextEvent.getServletContext();

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
        String propsLocation = prefix + context.getInitParameter("applicationConfig");
        String log4jLocation = prefix + context.getInitParameter("log4jConfig");

        try {
            //init the application configuration
            Props.initInstance(propsLocation);
            //init the log4j
            PropertyConfigurator.configure(Props.getProperties(log4jLocation));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }
}