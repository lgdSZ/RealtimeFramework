package com.mycompany.flink

import org.apache.flink.api.scala._

object BatchWordCount {
	def main(args: Array[String]): Unit = {
		//创建执行环境
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
		//从文件中读取数据
		val sourceDS: DataSet[String] = env.readTextFile("WordCount/input/words.txt")
		//分词之后，用groupby对单词进行分组，用sum进行聚合
		val resDS: AggregateDataSet[(String, Int)] = sourceDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
		//打印输出
		resDS.print()
	}

}
