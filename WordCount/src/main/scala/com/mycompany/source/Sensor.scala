package com.mycompany.source


import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
object Sensor {
	def main(args: Array[String]): Unit = {
		//创建运行环境
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
		val strenv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//1 从集合中创建流
		val str1: DataSet[SensorReading] = env.fromCollection(List(
			SensorReading("sensor_1", 1547718199, 35.80018327300259),
			SensorReading("sensor_6", 1547718201, 15.402984393403084),
			SensorReading("sensor_7", 1547718202, 6.720945201171228),
			SensorReading("sensor_10", 1547718205, 38.101067604893444)
		))

		//env.fromElements("flink",1,3.0,4L).print("test").setParallelism(1)

		//从kafka中读取数据
		//创建kafka相关配置
		val properties = new Properties()
		properties.setProperty("bootstrap.servers","hadoop112:9092")
		properties.setProperty("group.id","xi")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")

		val stream3: DataStream[String] = strenv.addSource(new FlinkKafkaConsumer011[String]("first",new SimpleStringSchema(),properties))

		stream3.print("stream3:").setParallelism(1)


		env.execute()

	}

}

case class SensorReading(id: String, timestamp: Long, temperature: Double)