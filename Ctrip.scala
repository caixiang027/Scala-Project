/**
  * Created by mlamp on 2017/7/12.
  */
import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Failure
import scala.util.Success
import scala.util.Try

// 携程游记一览Url，可变部分（1：地区 2：最大分页号（每页9篇游记））
object ctrip {
  val Url = "http://you.ctrip.com/travels/%s/s3-p%d.html"
  val ctripMap = Map(                        //设置的页数不能超过实际总共的页数，否则会一直延迟找不到下一页
    "国内" -> ("china110000", 400),
    "亚洲" -> ("asia120001", 500),
    "欧洲" -> ("europe120002", 300),
    "大洋洲" -> ("oceania120003", 200),
    "非洲" -> ("africa120006", 300),
    "南美洲" -> ("southamerica120005", 100),
    "北美洲" -> ("northamerica120004", 300),
    "南极洲" -> ("antarctica120481", 11)
  )

  def sleep(i: Long) = Thread.sleep(i)

  val aiAll, aiCnt, aiFail: AtomicInteger = new AtomicInteger(0)
  var map = new java.util.concurrent.ConcurrentHashMap[String, String]()

  // 解析单页的游记，过滤出“精华”，“美图”，“典藏”，“实用”四种类型游记
  def parseDoc(doc: Document) = {
    var allCnt, objCnt = 0
    for (e <- doc.select("a.journal-item.cf")) {
      var tn = ""
      if (!e.select("span.pic-tagico-1").isEmpty()) tn += "精"
      if (!e.select("span.pic-tagico-2").isEmpty()) tn += "美"
      if (!e.select("span.pic-tagico-3").isEmpty()) tn += "实"
      if (!e.select("span.pic-tagico-4").isEmpty()) tn += "典"
      if (tn != "") { // 只保留符合条件的数据
        println(tn + "\t"+ e.select("span.tips_a").html  + e.select("i.numview").html )
        map.put(e.attr("href"), e.attr("href") + "\t" //Url
          + e.select("dt.ellipsis").html + "\t" //标题
          + tn + "\t" //类型名(精|美|实|典)
          + e.select("dd.item-user").html.replaceAll("\n", "") + "\t" //作者+发表时间
          + e.select("dd.item-short").html + "\t" //摘要
          + e.select("span.tips_a").html + "\t" //天数+旅游时间+花费+同伴关系
          + e.select("span.tips_b").html + "\t" //tips_b
          + e.select("i.numview").html + "\t" //点击数
          + e.select("i.want").html + "\t" //点赞数
          + e.select("i.numreply").html); //回复数
        objCnt += 1
      }
      allCnt += 1
    }
    (allCnt, objCnt)
  }


  // 利用递归实现自动重试（重试100次，每次休眠30秒）
  def promiseGetUrl(times: Int = 100, delay: Long = 30000)(z: String, i: Int): Unit = {
    Try(Jsoup.connect(Url.format(z, i)).get()) match {
      case Failure(e) =>
        if (times != 0) {
          println(e.getMessage);
          aiFail.addAndGet(1);
          sleep(delay);
          promiseGetUrl(times - 1, delay)(z, i)
        } else throw e
      case Success(d) =>
        val (all, obj) = parseDoc(d);
        if (all == 0) {
          sleep(delay); promiseGetUrl(times - 1, delay)(z, i)
        } //携程跳转验证码走这里！
        aiAll.addAndGet(all);
        aiCnt.addAndGet(obj);
    }
  }

  // 并发集合多线程执行
  def concurrentCrawler(zone: String, maxPage: Int, threadNum: Int) = {
    val loopPar = (1 to maxPage).par
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum)) // 设置并发线程数
    loopPar.foreach(promiseGetUrl()(zone, _)) // 利用并发集合多线程同步抓取
    output(zone)
  }

  // 获取当前日期 (简单机能用Java就Ok)
  def getNowDate(): String = {
    new SimpleDateFormat("yyyyMMdd").format(new Date())
  }

  // 爬取内容写入文件
  def output(zone: String) = {
    val writer = new PrintWriter(new File(getNowDate() + "_" + zone ++ ".txt"))
    for ((_, value) <- map) writer.println(value)
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val Thread_Num = 30 //指定并发执行线程数
    val t1 = System.currentTimeMillis
    //全体抓取
    ctripMap.foreach {
      m =>
        concurrentCrawler(m._2._1, m._2._2, Thread_Num)
        map = new ConcurrentHashMap[String, String]();
    }
    //个别抓取
    //val tup = ctripMap("欧洲"); concurrentCrawler(tup._1, tup._2, Thread_Num)
    val t2 = System.currentTimeMillis
    println(s"抓取数：$aiCnt  重试数：$aiFail  耗时(秒)：" + (t2 - t1) / 1000)
  }
}


