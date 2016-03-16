package com.ai.paas.ipaas.rcs.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.rcs.param.FlowParam;
import com.ai.paas.ipaas.rcs.utils.FlowJdbcUtils;

/**
 * 从RCS的系统数据库表中获取流程的定义信息，供运行拓扑的时候使用,属于流程定义接口的另一种实现
 * @author ygz
 *
 */
public class FlowFromDb implements IFlowDefine
{

	public static final String DBDRIVER = "com.mysql.jdbc.Driver"; //系统目前只使用mysql。
	private Connection conn;
	private String flowName;
	private Long taskId;
	private Logger LOG = null;
	
	/**
	 * constructor, 实现数据库的连接
	 * @param url       url
	 * @param username  用户名
	 * @param password  密码
	 * @param aFlowName 拓扑流程的名称
	 */
	public FlowFromDb(String url, String username, String password, String aFlowName)
	{
		LOG = LoggerFactory.getLogger(aFlowName);

		flowName = aFlowName;
		taskId = 0L;

		try {
			Class.forName(DBDRIVER);
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );
			throw new RuntimeException("JDBC Exception");
		}
		
	}
	
	/**
	 * 定义流程，从数据库表中获取流程的信息
	 * @param args ： 流程执行时需要的命令行参数
	 * @param aBuilder： 流程定义抽象类
	 */
	@Override
	public void define(String[] args, Module aBuilder, FlowParam aParams) 
	{
		PreparedStatement stmt = null;  
		ResultSet result = null;
		
		//设置参数类
		try {
			stmt = conn.prepareStatement("select id, num_workers from rcs_task_info where name = ?");
			stmt.setString(1, flowName);
			result = stmt.executeQuery();
		} catch (SQLException e) { 
			LOG.error("SQLException : [" + e.getMessage() + "-" + e.getCause() + "]" );
			
			return ;
		}
		
		int numWorkers = 0;
		try {
			if (result.next())
			{
				taskId = result.getLong("id");
			    numWorkers = result.getInt("num_workers");   
			    
			    aParams.setFlowName(flowName);
			    aParams.setNumWorkers(numWorkers);
			    				
			}else{
				aParams.setFlowName("");
			    aParams.setNumWorkers(0);
			}
			
		    System.out.println(this.getClass().getName() + "- flowName = [" + flowName + "] numWorkers = [" + numWorkers +"]\n");  
			
			result.close();
			result = null;
		} catch (SQLException e) { 
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return ;
		}

		//设置输入类
		try {
			stmt = conn.prepareStatement("select spout_name, spout_class_name, threads from rcs_spout_info where task_id = ? ");
			stmt.setLong(1, taskId);
			result = stmt.executeQuery();
		} catch (SQLException e) {
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return;
		}

		try {
			while (result.next())
			{
			    String name = result.getString("spout_name");  
			    String class_name = result.getString("spout_class_name");  
			    int threads = result.getInt("threads");  //线程数量

				aBuilder.setInput(name, class_name, null, threads);
			}

			result.close();
			result = null;
		} catch (SQLException e) {
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return;
		}
		 
		//设置处理类
		try {
			stmt = conn.prepareStatement("select bolt_name, bolt_class_name, threads, grouping_types, grouping_spout_or_blots from rcs_bolt_info where task_id = ? ");
			stmt.setLong(1, taskId);
			result = stmt.executeQuery();
		} catch (SQLException e) { 
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return ;
		}

		try {
			while (result.next())
			{  
			    String name = result.getString("bolt_name");
			    String className = result.getString("bolt_class_name");  
			    int threads = result.getInt("threads");   //线程数量
			    String groupingTypes = result.getString("grouping_types");
			    String grouping_spout_or_blots = result.getString("grouping_spout_or_blots");// 前一个节点
			    
			    if(groupingTypes.equals("fields")){
					aBuilder.setProcessor(name, className, threads, null, groupingTypes, null, grouping_spout_or_blots);
			    	
			    }else if(groupingTypes.equals("shuffle")){
					aBuilder.setProcessor(name, className, groupingTypes, grouping_spout_or_blots); 	
			    }else{
			    	System.out.print("Wrong group type : [" + groupingTypes + "]\n");
			    }
			   				
			    System.out.println("bolt_name = [" + name + "]\n");  
			}
			
			result.close();
			result = null;
		} catch (SQLException e) { 
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return;
		}
		
		//关闭相关资源
		try {
			if(stmt != null){
				stmt.close();
				stmt = null;
			}
		} catch (SQLException e) { 
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

			return; 
		} 
		
		closeConn();
		
	}

	/**
	 * 设置流程的配置信息
	 */
	@Override
	public void configure(String[] args, FlowConfig aConfig) 
	{ 
		
	}

	/**
	 * 关闭数据库连接
	 */
	public void closeConn() 
	{
		try{
			if (conn != null) 
			{
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			LOG.error("Exception : [" + e.getMessage() + "-" + e.getCause() + "]" );

		}
	}
}
