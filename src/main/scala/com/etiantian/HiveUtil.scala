package com.etiantian

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by yuchunfan on 2017/8/23.
  */
object HiveUtil {

  object SQLHiveContextSingleton {
    @transient private var instance: HiveContext = _

    def getInstance(sparkContext: SparkContext): HiveContext = {
      synchronized {
        if (instance == null)
          instance = new HiveContext(sparkContext)
        instance
      }
    }
  }
}
