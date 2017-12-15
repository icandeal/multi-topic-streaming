package com.etiantian

import java.io.UnsupportedEncodingException

import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.log4j.LogManager

/**
  * Created by yuchunfan on 2017/9/7.
  */
class MyZkSerializer extends ZkSerializer {
  val logger = LogManager.getLogger("MyZkSerializer")
  override def serialize(o: scala.Any): Array[Byte] = {
    try
      return String.valueOf(o).getBytes("UTF-8")
    catch {
      case e: UnsupportedEncodingException => logger.error(e)
    }
    return null
  }

  override def deserialize(bytes: Array[Byte]): AnyRef = {
    try
      return new String(bytes,"UTF-8")
    catch {
      case e: UnsupportedEncodingException =>
        logger.error(e)
    }
    return null
  }
}
