package com.ai.paas.ipaas.rcs.utils;

import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.gson.Gson;

public class JsonUtils {
	/**
	 * 创建JSONObject
	 */
	public static JSONObject createJSONObject(String proName,String proValue){
		JSONObject jsonObject=new JSONObject();
		jsonObject.put(proName, proValue);
		return jsonObject;
	}
	/**
	 * 创建JSONObject
	 */
	public static JSONObject createJSONObject(String... str ){
		if (str.length%2==1) {
			throw new RuntimeException("参数数量不对，为偶数");
		}
		JSONObject jsonObject=new JSONObject();
		for (int i = 0; i < str.length; i=i+2) {
			jsonObject.put(str[i], str[i+1]);
		}
		return jsonObject;
	}
	/**
	 * 创建JSONObject
	 */
	public static JSONObject parse(String str ){
//		JSONObject jsonObject = new JSONObject(str);
		JSONObject jsonObject = new JSONObject();
		return jsonObject;
	}
	/**
	 * 创建JSONObject
	 */
	public static JSONObject parse(Object object){
		JSONObject jsonObject = new JSONObject();
//		JSONObject jsonObject = new JSONObject(object);
		return jsonObject;
	}
	/**
	 * 创建JSONArray
	 */
	public static JSONArray parseArray(List<String> list){
//		JSONArray jsonArray=new JSONArray(list);
		JSONArray jsonArray = new JSONArray();
		return jsonArray;
	}
	/**
	 * 创建JSONArray
	 */
	public static JSONArray parseArray(Set<String> list){
//		JSONArray jsonArray = new JSONArray(list);
		JSONArray jsonArray = new JSONArray();
		return jsonArray;
	}
	/**
	 * 返回JSON格式字符串
	 */
	public static String toJsonString(Object object){
		Gson gson = new Gson();
		return gson.toJson(object);
	}
}
