package chapter7

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object AverageUDAF extends UserDefinedAggregateFunction {
  //设置该聚合函数输入的数据类型
  override def inputSchema: StructType = {
    StructType(
      StructField("numInput", DoubleType, nullable = true)
        :: Nil
    )
  }

  //设置聚合过程中缓冲区的数据类型
  override def bufferSchema: StructType = {
    StructType(
      StructField("buffer1", DoubleType, nullable = true)
        :: StructField("buffer2", LongType, nullable = true)
        :: Nil
    )
  }

  //聚合函数最终返回的数据类型
  override def dataType: DataType = DoubleType

  //聚合函数的输入与输出的数据类型是否一致
  override def deterministic: Boolean = true

  //初始化聚合缓冲区中的数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0L)
  }

  //将当前行的值累加到缓冲区buffer中，每行调用一次
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getDouble(0) + input.getDouble(0))
    buffer.update(1, buffer.getLong(1) + 1)
  }

  //合并两个缓冲区，并将更新后的缓冲区值存储到buffer1中
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
  }

  //将所有确定的缓冲区中的数据做最后一次、会得出UDAF运行结果的运算
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
