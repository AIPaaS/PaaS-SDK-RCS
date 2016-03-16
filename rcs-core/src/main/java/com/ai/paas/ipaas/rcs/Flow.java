package com.ai.paas.ipaas.rcs;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.rcs.common.FlowConfig;
import com.ai.paas.ipaas.rcs.common.FlowFromDb;
import com.ai.paas.ipaas.rcs.common.IFlowDefine;
import com.ai.paas.ipaas.rcs.common.Module;
import com.ai.paas.ipaas.rcs.param.FlowParam;
 

import backtype.storm.LocalCluster; 
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
 
/**
 * 拓扑的主类
 * @author ygz
 *
 */
public class Flow 
{
	public static final int SKIP_ARGS_COUNT = 7;
	private Logger LOG = null;
	String flowID = null;        //即storm中拓扑的名称
	String flowCLassName = null;
	String flowInfoSrc = null;
	String mysqlUrl = null;
	String mysqlUserName = null;
	String mysqlPassWord = null;
	String runMode = null;
	String[] flowArgs = null;
	
    /**
     * 定义拓扑定义的获取来源的枚举类型 
     */
    public enum TPDefSrc
    {
       // 利用构造函数传参 
       USR  (1),  SYS  (2); 

       //  定义私有变量 
       private int nCode ; 

       //  构造函数，枚举类型只能为私有 
       private TPDefSrc( int _nCode) { 
           this.nCode  = _nCode; 
       } 

       @Override 
       public String toString() { 
           return String.valueOf ( this.nCode ); 
       } 
    }

	/**
	 * default constructor
	 */
	public Flow(String[] aFlowArgs)
	{
		flowArgs = aFlowArgs;
		
		LOG = LoggerFactory.getLogger(flowArgs[0]);
		
		flowID = flowArgs[0];        //即storm中拓扑的名称
		flowCLassName = flowArgs[1];
		flowInfoSrc = flowArgs[2];
		mysqlUrl = flowArgs[3];
		mysqlUserName = flowArgs[4];
		mysqlPassWord = flowArgs[5];
		runMode = flowArgs[6];
		

	}
	
	public void run()
	{
		Module modules = new Module(flowID);
		FlowConfig flowConfigs = new FlowConfig();
		
		FlowParam flowParams = new FlowParam();


		//从jar包里面搜索该接口的实现,改为从命令行输入
		//通过java反射，创建输入类的实例
		Class<IFlowDefine> aClass = null;  //WordCounter.TopologyMain
		IFlowDefine aFlowDefine = null;
		try {
			aClass = (Class<IFlowDefine>) Class.forName(flowCLassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
 
		try {
			aFlowDefine = (IFlowDefine)aClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		//把前SKIP_ARGS_COUNT个参数去掉，这SKIP_ARGS_COUNT个参数之后，才是用户需要通过命令行输入的参数
		String[] userArgs = new String[flowArgs.length - SKIP_ARGS_COUNT];
		for(int i = 0; i<flowArgs.length - SKIP_ARGS_COUNT; i++){
			userArgs[i] = flowArgs[i + SKIP_ARGS_COUNT];
		}

		//获取用户定义的拓扑流程的相关信息，保存到函数的参数对象中，以便后面引用
		aFlowDefine.define(userArgs, modules, flowParams);

		//判断拓扑流程信息的来源
		if(flowInfoSrc.equalsIgnoreCase("USR")){
			//使用用户定义的拓扑流程 自测时使用
			flowParams.setFlowName(flowID);
			
		}else if(flowInfoSrc.equalsIgnoreCase("SYS")){
			//使用系统参数表中定义的拓扑流程 正式运行的时候使用，即用数据库中存储的信息覆盖用户定义的信息
			FlowFromDb aFlowFromDb = new FlowFromDb(mysqlUrl, mysqlUserName, mysqlPassWord, flowID);
			aFlowFromDb.define(flowArgs, modules, flowParams); 
		}else{
			System.out.println("NOT recognised parameter:[" + flowInfoSrc + "]");
			return ;
		}

		//用定义好的输入类和处理类构建拓扑流程图
		modules.build(flowID);

		aFlowDefine.configure(userArgs, flowConfigs);
		flowConfigs.setNumWorkers(flowParams.getNumWorkers());

		//判断运行模式
		if(runMode.equalsIgnoreCase("LOCAL")){
			//在本机运行拓扑
		    LocalCluster cluster = new LocalCluster();                
		    cluster.submitTopology(flowParams.getFlowName(), flowConfigs.getConf(), modules.getBuilder().createTopology());   

		    try {
				Thread.sleep(100000);
			} catch (InterruptedException e) { 
				e.printStackTrace();
			}
		    cluster.shutdown();    
		}else if(runMode.equalsIgnoreCase("REMOTE")){
			//在远程服务器执行
			try {
				StormSubmitter.submitTopology(flowParams.getFlowName(), flowConfigs.getConf(), modules.getBuilder().createTopology());
			} catch (AlreadyAliveException e) { 
				e.printStackTrace();
			} catch (InvalidTopologyException e) { 
				e.printStackTrace();
			}
		}else{
			System.out.println("NOT recognised parameter 2:[" + runMode + "]. The right one should be LOCAL or REMOTE");
			return;
		}

		return;
	}

	/**
	 * 拓扑的启动入口
	 * @param args
	 * args[0]:流程标识
	 * args[1]：IFlowDefine由用户实例化之后的类名称，
	 * args[2]：第二个参数是拓扑定义的获取来源，取值：USR-用户定义的拓扑；SYS-从RCS的配置表中获取拓扑的定义
	 * args[3]:mysql URL
	 * args[4]:mysql user name
	 * args[5]:mysql password
	 * args[6]:mode,拓扑的运行模式，取值： LOCAL， REMOTE
	 * args[7...] 用户设定的在处理流程中需要使用到的命令行参数，这些参数会传入到拓扑中
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		if(args.length < SKIP_ARGS_COUNT)
		{
			System.out.println("Wrong command line parameters. \n ");
			return ;
		}

		for(int i = 0; i<args.length ; i++){
			System.out.println("Parameters: args[" + i +"] = [" + args[i]+ "]");
		}

		Flow aFlow = new Flow(args);
		aFlow.run();
		
    } 
}
