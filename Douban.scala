/**
  * Created by mlamp on 2017/7/11.
  */
import java.io.{File, PrintWriter}
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.jsoup.Jsoup
import org.jsoup.nodes.Document


import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * Created by peace on 2017/3/5.
  */
object Douban {
  val URL = "https://movie.douban.com/tag/%s?start=%d&type=T"
  //访问的链接
  //需要抓取的标签和页数
  val tags = Map(
    "经典" -> 5, //tag，页数
    "爱情" -> 5,
    "动作" -> 5,
    "剧情" -> 5,
    "悬疑" -> 5,
    "文艺" -> 5,
    "搞笑" -> 5,
    "战争" -> 5
  )

  //解析Document，需要对照网页源码进行解析
  def parseDoc(doc: Document, movies: ConcurrentHashMap[String, String]) = {
    var count = 0
    for (elem <- doc.select("tr.item")) {
      movies.put(elem.select("a.nbg").attr("title"), elem.select("a.nbg").attr("title") + "\t" //标题
        + elem.select("a.nbg").attr("href") + "\t" //豆瓣链接
        // +elem.select("p.pl").html+"\t"//简介
        + elem.select("span.rating_nums").html + "\t" //评分
        + elem.select("span.pl").html //评论数
      )
      count += 1
    }
    count
  }

  //用于记录总数，和失败次数
  val sum, fail: AtomicInteger = new AtomicInteger(0)
  /**
    *  当出现异常时10s后重试,异常重复100次
    * @param delay：延时时间
    * @param url：抓取的Url
    * @param movies：存取抓到的内容
    */
  def requestGetUrl(times: Int = 100, delay: Long = 10000)(url: String, movies: ConcurrentHashMap[String, String]): Unit = {
    Try(Jsoup.connect(url).get()) match {//使用try来判断是否成功和失败对网页进行抓取
      case Failure(e) =>
        if (times != 0) {
          println(e.getMessage)
          fail.addAndGet(1)
          Thread.sleep(delay)
          requestGetUrl(times - 1, delay)(url, movies)
        } else throw e
      case Success(doc) =>
        val count = parseDoc(doc, movies);
        if (count == 0) {
          Thread.sleep(delay);
          requestGetUrl(times - 1, delay)(url, movies)
        }
        sum.addAndGet(count);
    }
  }

  /**
    * 多线程抓取
    * @param url:原始的Url
    * @param tag：电影标签
    * @param maxPage：页数
    * @param threadNum：线程数
    * @param movies：并发集合存取抓到的内容
    */
  def concurrentCrawler(url: String, tag: String, maxPage: Int, threadNum: Int, movies: ConcurrentHashMap[String, String]) = {
    val loopPar = (0 to maxPage).par
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum)) // 设置并发线程数
    loopPar.foreach(i => requestGetUrl()(url.format(URLEncoder.encode(tag, "UTF-8"), 20 * i), movies)) // 利用并发集合多线程同步抓取:遍历所有页
    saveFile1(tag, movies)
  }

  //直接输出
  def saveFile(file: String, movies: ConcurrentHashMap[String, String]) = {
    val writer = new PrintWriter(new File(new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    for ((_, value) <- movies) writer.println(value)
    writer.close()
  }

  // 排序输出到文件
  def saveFile1(file: String, movies: ConcurrentHashMap[String, String]) = {
    val writer = new PrintWriter(new File(new SimpleDateFormat("yyyyMMdd").format(new Date()) + "_" + file ++ ".txt"))
    val col = new ArrayBuffer[String]();
    for ((_, value) <- movies)
      col += value;
    val sort = col.sortWith(
      (o1, o2) => {
        val s1 = o1.split("\t")(2);
        val s2 = o2.split("\t")(2);
        if (s1 == null || s2 == null || s1.isEmpty || s2.isEmpty) {
          true
        } else {
          s1.toFloat > s2.toFloat
        }
      }
    )
    sort.foreach(writer.println(_))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val Thread_Num = 30 //指定并发执行线程数
    val t1 = System.currentTimeMillis
    for ((tag, page) <- tags)
      concurrentCrawler(URL, tag, page, Thread_Num, new ConcurrentHashMap[String, String]())//并发抓取
    val t2 = System.currentTimeMillis
    println(s"抓取数：$sum  重试数：$fail  耗时(秒)：" + (t2 - t1) / 1000)
  }
}
