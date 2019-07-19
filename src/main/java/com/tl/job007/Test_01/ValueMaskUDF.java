package com.tl.job007.Test_01;
/*
 * 功能实现 : 输入字符串 是两个作为输出,多余用...表示
 */
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

public class ValueMaskUDF extends UDF{
           public String evaluate(String input,int maxSaveStringLength,String replaceSign){
        	   if (input.length() <=maxSaveStringLength ) {
				return input;
			}
        	   return input.substring(0,maxSaveStringLength)+replaceSign;
           }
           public static void main(String[] args) {
			System.out.println( new ValueMaskUDF().evaluate("河北省",2,"..."));
		}
}
  