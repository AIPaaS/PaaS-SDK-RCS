package com.ai.paas.ipaas.rcs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 流程配置表的访问工具
 * @author ygz
 *
 */
public class FlowJdbcUtils 
{
	public static final String DBDRIVER = "com.mysql.jdbc.Driver"; //系统目前只使用mysql。
	
	private Connection conn;

	/**
	 * 在构造函数里面创建数据库连接
	 * @param url
	 * @param username
	 * @param password
	 */
	public FlowJdbcUtils(String url, String username, String password)
	{
		try {
			Class.forName(DBDRIVER);
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			throw new RuntimeException("JDBC异常");
		}

	}
	
	/**
	 * 关闭数据库连接
	 */
	private void closeConn() 
	{
		try {
//			if (rs != null) {
//				rs.close();
//				rs = null;
//			}
//			
//			if (stmt != null) {
//				stmt.close();
//				stmt = null;
//			}
			
			if (conn != null) {
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
