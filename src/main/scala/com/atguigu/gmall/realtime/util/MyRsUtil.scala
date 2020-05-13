package com.atguigu.gmall.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Index, Search, SearchResult}

import scala.collection.mutable.ListBuffer

object MyRsUtil {

  private    var  factory:  JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop105:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())

  }

  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient

    //val index = new Index.Builder(Movie(4,"红海战役",9.0)).index("movie_chn").`type`("movie").id("4").build()
    val query = "{\n  \"query\": {\n    \"match\": {\n      \"name\": \"红海行动\"\n    }\n  }\n}"

    val search = new Search.Builder(query).addIndex("movie_chn").addType("movie").build()
    val result = jest.execute(search)
    val movieResultList: util.List[SearchResult#Hit[Movie, Void]] = result.getHits(classOf[Movie])
    import  scala.collection.JavaConversions._

    val movieList=ListBuffer[Movie]()
    for (hit <- movieResultList ) {
      val movie: Movie = hit.source
      movieList+= movie
    }
    println(movieList.mkString("\n"))

    jest.close()
  }

  case class Movie(id:Long,name:String,doubanScore:Double){

  }

}
