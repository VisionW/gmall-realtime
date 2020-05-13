package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer



object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "GMALL_START"
    val groupId = "GMALL_DAU_CONSUMER"

    val startInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //startInputStream.map(_.value).print(1000)


    val jsonObjDStream: DStream[JSONObject] = startInputStream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    val startJsonObjWithDauDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions { jsonObjItr =>
      val jedis = RedisUtil.getJedisClient
      //val jedis = new Jedis("hadoop105",6000)
      val jsonObjList = jsonObjItr.toList
      println("去重前："+jsonObjList.size)
      val jsonObjFilteredList = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonObjList) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))
        val dauKey = "dau:" + dateStr
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        if(isFirstFlag==1L){
          jsonObjFilteredList+=jsonObj
        }
      }

      jedis.close()
      println("去重后："+jsonObjFilteredList.size)
      jsonObjList.toIterator

    }
    //startJsonObjWithDauDStream.print(1000)
    //变换结构

    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDStream.map { jsonObj =>
      val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(jsonObj.getLong("ts")))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)

      DauInfo(commonJsonObj.getString("mid"),
        commonJsonObj.getString("uid"),
        commonJsonObj.getString("ar"),
        commonJsonObj.getString("ch"),
        commonJsonObj.getString("vc"),
        dt, hr, mi, jsonObj.getLong("ts")
      )

    }

    //插入gmall1122_dau_info
    dauInfoDstream.foreachRDD {rdd=>
      rdd.foreachPartition { dauInfoItr =>
        val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map { dauInfo => (dauInfo.mid, dauInfo) }
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
        val indexName = "gmall1122_dau_info_" + dt
        MyEsUtil.saveBulk(dataList, indexName)
      }
    }

    
    println(1)


    ssc.start()
    ssc.awaitTermination()

  }
}
