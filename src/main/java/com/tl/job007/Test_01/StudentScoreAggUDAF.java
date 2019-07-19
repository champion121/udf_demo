package com.tl.job007.Test_01;



import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import com.sun.istack.logging.Logger;

public class StudentScoreAggUDAF extends UDAF { // 日志对象初始化
	public static Logger logger = Logger.getLogger(StudentScoreAggUDAF.class);

	// 静态类实现 UDAFEvaluator
	public static class Evaluator implements UDAFEvaluator {
		// 设置成员变量，存储每个统计范围内的总记录数
		private Map<String, String> courseScoreMap;

		// 初始化函数,map 和 reduce 均会执行该函数,起到初始化所需要的 变量的作用
		public Evaluator() {
			init();
		} // 初始化函数间传递的中间变量

		public void init() {
			courseScoreMap = new HashMap<String, String>();
		} // map 阶段，返回值为 boolean 类型，当为 true 则程序继续执行， 当为 false 则程序退出

		public boolean iterate(String course, String score) {
			if (course == null || score == null) {
				return true;
			}
			courseScoreMap.put(course, score);
			return true;
		}

		/**
		 * * 类似于 combiner,在 map 范围内做部分聚合，将结果传给 merge 函数中的形参 mapOutput 如果需要聚合，则对
		 * iterator 返回的结果处理，否则直接返回 iterator 的结果即可
		 */
		public Map<String, String> terminatePartial() {
			return courseScoreMap;
		}

		// reduce 阶段，用于逐个迭代处理 map 当中每个不同 key 对应 的 terminatePartial 的结果
		public boolean merge(Map<String, String> mapOutput) {
			this.courseScoreMap.putAll(mapOutput);
			return true;
		} // 处理 merge 计算完成后的结果，即对 merge 完成后的结果做最后 的业务处理

		public String terminate() {
			return courseScoreMap.toString();
		}
	}
}