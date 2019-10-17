import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 用户行为分析
  * 最近一小时热门商品topN / 每5分钟统计一次
  * 控制台输出  或 写到kafka
  */

// 输入数据样例类(用户id,商品id,商品类别id,用户行为,时间戳/ms)
//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 输出数据样例类(商品id,窗口闭合时间,数量)
//case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems2{
    def main(args: Array[String]): Unit = {
        // 创建一个env
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 显式地定义事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val dataStream = env.socketTextStream("localhost", 8001)
        val result =  dataStream.map(line => {
            val linearray = line.split(",")
           /* val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val timestamp =  simpleDateFormat.format(linearray(4))*/
              UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3),linearray(4).toLong)
          })
          //指定时间戳和watermark,使用自动创建
          .assignAscendingTimestamps(_.timestamp * 1000) // flink auto create timestamp & watermark
          .filter(_.behavior == "pv")
          .keyBy("itemId")
          //.timeWindow(Time.hours(1), Time.seconds(30)) //这里的一小时 30s 窗口 是对数据的事件时间来说的
          .timeWindow(Time.minutes(1), Time.seconds(5))
          .aggregate(new CountAgg(), new WindowResultFunction())
          .keyBy("windowEnd")
          .process(new TopNHotItems(3)) //process 执行自定义函数

        result.print()
        // 调用execute执行任务
        env.execute("Hot Items Job")
    }

    // 自定义实现聚合函数
    class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        override def createAccumulator(): Long = 0L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    // 自定义实现Window Function，输出ItemViewCount格式
    class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
            val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
            val count = input.iterator.next()
            out.collect(ItemViewCount(itemId, window.getEnd, count))
        }
    }

    // 自定义实现process function
    class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

        // 定义状态ListState
        private var itemState: ListState[ItemViewCount] = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            // 命名状态变量的名字和类型
            val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
            itemState = getRuntimeContext.getListState(itemStateDesc)
        }

        override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
            itemState.add(i)
            // 注册定时器，触发时间定为 windowEnd + 1，触发时说明window已经收集完成所有数据
            context.timerService.registerEventTimeTimer(i.windowEnd + 1)
        }

        // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
            // 获取所有的商品点击信息
            val allItems: ListBuffer[ItemViewCount] = ListBuffer()
            import scala.collection.JavaConversions._
            for (item <- itemState.get) {
                allItems += item
            }
            // 清除状态中的数据，释放空间
            itemState.clear()

            // 按照点击量从大到小排序，选取TopN
            val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

            // 将排名数据格式化，便于打印输出
            val result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

            for (i <- sortedItems.indices) {
                val currentItem: ItemViewCount = sortedItems(i)
                // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
                result.append("No").append(i + 1).append(":")
                  .append("  商品ID=").append(currentItem.itemId)
                  .append("  浏览量=").append(currentItem.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率
            Thread.sleep(1000)
            out.collect(result.toString)
        }
    }

}
