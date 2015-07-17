package com.netease.hz.dao;

import com.netease.hz.model.Schema;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Created by zhifei on 3/20/15.
 */
public class SchemaLoader extends MySqlMetaLoader {
    private static final Logger logger = Logger.getLogger(SchemaLoader.class);


    /**
     * Persist a xml file.
     * @param path
     * @return The id of the newly persisted xml file
     */
    public long addSchema(String path) {
        BigInteger id = new BigInteger("-1");
        QueryRunner queryRunner = this.createQueryRunner();

        String filename = getFileNameFromPath(path);
        if(filename == null || filename == "") {
            logger.error("File path error. Paht="+path);
            return -1;
        }

        String sql = "INSERT INTO `schema` (`filename`,`filelocation`) VALUES (?,?)";
        Connection conn = null;
        try {
            conn = this.getDBConnection(true);
            queryRunner.update(conn, sql, filename, path);
            id = (BigInteger)queryRunner.query(conn, "SELECT LAST_INSERT_ID()", new ScalarHandler(1));

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }

        return id.longValue();
    }

    /**
     * Get the schema object by its id
     * @param schemaId
     * @return
     */
    public Schema getSchema(String schemaId){
        String sql = "select `filename`,`filelocation` from `schema` where `id`=?";
        Schema schema = new Schema();
        QueryRunner qr = this.createQueryRunner();
        Connection conn = null;
        try {
            conn = this.getDBConnection(true);
            Map<String, Object> map = qr.query(conn, sql, new MapHandler(), schemaId);
            schema.setName((String)map.get("filename"));
            schema.setFileLocation((String)map.get("filelocation"));

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return schema;

    }


    private String getFileNameFromPath(String filePath) {
        return filePath.substring(filePath.lastIndexOf("/") + 1);
    }
    
    public String praseXml(String xml_path, String query_cube, List<String> query_dimension) throws IOException {
    	
        List<String> filed_name = new ArrayList<String>();
        List<String> from = new ArrayList<String>();
        List<String> where = new ArrayList<String>();
		
		// step 1:获得DOM解析器工厂
        // 工厂的作用是创建具体的解析器
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        // step 2：获得具体的dom解析器
        DocumentBuilder db = null;
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			logger.error(e.getMessage());
		}

        // step 3:解析一个xml文档，获得Document对象（根节点）
        // 此文档放在项目目录下即可
        Document document = null;
		try {
			document = db.parse(new File(xml_path));
		} catch (SAXException e) {
			logger.error(e.getMessage());
		}

        // 根据标签名访问节点
        NodeList cube_list = document.getElementsByTagName("Cube");
        
        Element cube_element = null;
        // 遍历每一个节点
        for (int i = 0; i < cube_list.getLength(); ++i)
        {     
            // 获得cube元素，将节点强制转换为元素
            cube_element = (Element) cube_list.item(i); 
            if(cube_element.getAttribute("name").equalsIgnoreCase(query_cube)) break;
        }
        //获取事实表的名称
        String fact_table = cube_element.getElementsByTagName("Table").item(0).getAttributes().getNamedItem("name").getNodeValue();
        fact_table = "`"+fact_table+"`";
        from.add(fact_table);
    
        // 获取子元素：子元素title只有一个节点，之后通过getNodeValue方法获取节点的值
        NodeList Dimension_list = cube_element.getElementsByTagName("Dimension");
        for(int k=0;k<query_dimension.size();k++) {
        	Element dimension = null;
            for(int i=0;i<Dimension_list.getLength();i++) {
        	   dimension = (Element) Dimension_list.item(i);
        	   String dimension_name = dimension.getAttribute("name");
        	   if(query_dimension.get(k).equals(dimension_name)) break;
            }
        	String table_name = dimension.getElementsByTagName("Table").item(0).getAttributes().getNamedItem("name").getNodeValue();
        	table_name = "`"+table_name+"`"; 
        	from.add(table_name);
        	String foreignKey = "`"+dimension.getAttribute("foreignKey")+"`";
        	String condition = fact_table+"."+foreignKey+"="+table_name+"."+foreignKey;
        	where.add(condition);
        	NodeList level_list = dimension.getElementsByTagName("Level");
        	for(int j=0;j<level_list.getLength();j++) {
        		String level_column = "`"+level_list.item(j).getAttributes().getNamedItem("column").getNodeValue()+"`";
        		filed_name.add(table_name+"."+level_column);
        	}
    
        
        }
        String select_statement = " SELECT "+filed_name.get(0);
        for(int i=1;i<filed_name.size();i++)
        	select_statement+=","+filed_name.get(i);
        String from_statement = " FROM "+from.get(0);
        for(int i=1;i<from.size();i++)
        	from_statement+=","+from.get(i);
        String where_statement = " WHERE "+where.get(0);
        for(int i=1;i<where.size();i++)
        	where_statement+=" and "+where.get(i);
        String groupby_statement = " GROUP BY "+filed_name.get(0);
        for(int i=1;i<filed_name.size();i++)
        	groupby_statement+=","+filed_name.get(i);
        String sql=select_statement+from_statement+where_statement+groupby_statement;
    	return sql;
    }

}
