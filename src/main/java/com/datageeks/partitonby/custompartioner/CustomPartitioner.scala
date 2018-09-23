package com.datageeks.partitonby.custompartioner

import org.apache.spark.Partitioner

class CustomPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    if (key.toString().startsWith("A"))
      return 0;
    else
      return 1;
  }
}