package org.inh3rit.spark.kafka

import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.compress.utils.Charsets

class MyZkSerializer extends ZkSerializer {
  override def serialize(o: scala.Any): Array[Byte] = String.valueOf(o).getBytes(Charsets.UTF_8)

  override def deserialize(bytes: Array[Byte]): AnyRef = new String(bytes, Charsets.UTF_8)
}
