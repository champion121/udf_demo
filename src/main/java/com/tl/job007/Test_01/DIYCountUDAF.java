package com.tl.job007.Test_01;

import io.netty.util.collection.IntObjectHashMap;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import com.sun.istack.logging.Logger;

/**
 * 1,自定义一个java类 2,继承UDAF类 3,内部定义一个静态的类,实现UDAFEvaluator接口 4,实现方法
 * init,iterate,terminatePartial,merge,terminate 共 5 个方 法
 * 
 * @author win10
 *
 */
public class DIYCountUDAF extends UDAF {
	// 日志对象初始化,,是访问类有输出日志的能力
	public static Logger logger = Logger.getLogger(DIYCountUDAF.class);

	// 静态类实现UDAFEvaluator
	public static class Evaluator implements UDAFEvaluator {
		// 设置成员变量,存储每个统计范围内的总记录
		private int totalRecords;

		// 初始化函数,map和reduce均会执行该函数,起到初始化所需要的变量作用
		public Evaluator() {
			init();
		}

		// 初始化,初始值为0,并日志记录下相应输出
		@Override
		public void init() {
			totalRecords = 0;
			logger.info("init totalRecords=" + totalRecords);

		}

		// map 阶段，返回值为 boolean 类型，当为 true 则程序继续执行， 当为 false 则程序退出
		public boolean iterate(String input) {
			// 当 input 输入不为空的时候，即为有值存在,即为存在 1 行，故 做+1 操作
			if (input != null) {
				totalRecords += 1;
			}
			// 输出当前组处理到第多少条数据了
			logger.info("iterate totalRecords=" + totalRecords);
			return true;
		}

		/**
		 * 类似于 combiner,在 map 范围内做部分聚合，将结果传给 merge 函数中的形参 mapOutput 如果需要聚合，则对
		 * iterator 返回的结果处理，否则直接返回 iterator 的结果即可
		 */
		public int terminatePartial() {
			logger.info("terminatePartial totalRecords=" + totalRecords);
			return totalRecords;
		}

		// reduce 阶段，用于逐个迭代处理 map 当中每个不同 key 对应的 terminatePartial 的结果
		public boolean merge(int mapOutput) {
			totalRecords += mapOutput;
			logger.info("merge totalRecords=" + totalRecords);
			return true;
		}

		// 处理 merge 计算完成后的结果，此时的 count 在 merge 完成时候，
		// 结果已经得出，无需再进一次对整体结果做处理，故直接返回即可
		public int terminate() {
			logger.info("terminate totalRecords=" + totalRecords);
			return totalRecords;
		}

	}

}
