package com.mycompany.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlowWordCount {
	def main(args: Array[String]): Unit = {
		//利用flink的parameterTool,从外部命令中获取参数
		val params: ParameterTool = ParameterTool.fromArgs(args)
		val host: String = params.get("host")
		val port: Int = params.getInt("port")
		//创建执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//接收socket文本流
		val sourceDS: DataStream[String] = env.socketTextStream(host,port)
		//分词之后，用groupby对单词进行分组，用sum进行聚合
		val resDS: DataStream[(String, Int)] = sourceDS.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
		//打印输出
		resDS.print().setParallelism(1)


		//启动Executor,执行任务
		env.execute("FlowWordCount")
	}

}
