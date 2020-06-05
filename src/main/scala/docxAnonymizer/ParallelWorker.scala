package docxAnonymizer

import java.io.{File, PrintWriter}
import java.util
import java.util.Optional
import java.util.regex.Matcher

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import Control._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.io.Source
import scala.collection.mutable

class ParallelWorker(val runNodes: util.List[AnyRef],
                     val peopleToMinimize: List[Persona],
                     val peopleToKeepUnchanged: List[Persona],
                     val keepViaConfig: Option[String],
                     val debug: Boolean,
                     val sc: SparkContext) extends Worker with Serializable {

  if (debug) {
    Persona.setDebug(true)
    Elaborator.setDebug(true)
    AnonymUtils.setDebug(true)

  }

  private val elaborator: Elaborator = keepViaConfig match {
    case None =>
      new Elaborator(runNodes, peopleToMinimize.asJava, peopleToKeepUnchanged.asJava)
    case Some(keepViaConfigPath: String) =>
      new Elaborator(runNodes, peopleToMinimize.asJava, peopleToKeepUnchanged.asJava, keepViaConfigPath)
  }

  def work(): Unit = {
    val allOriginal: List[String] = AnonymUtils.getOriginal(elaborator).asScala.toList
    val allPreprocessed: List[String] = AnonymUtils.preprocess(elaborator).asScala.toList
    val allEntryPoints: Map[Int, Seq[EntryPoint]] = AnonymUtils.getEntryPoints(elaborator).asScala.groupBy(_.getIndex_PlainText)
    val allUnchangeable: Map[Int, Seq[EntryPoint]] = AnonymUtils.getUnchangeable(elaborator).asScala.groupBy(_.getIndex_PlainText)
    val inputs: Iterable[(Int, String, String, Seq[EntryPoint], Seq[EntryPoint])] =
      allEntryPoints map {
        case (idx, eps) =>
          (idx, allOriginal(idx), allPreprocessed(idx), eps, allUnchangeable.getOrElse(idx, Seq[EntryPoint]()))
      }

    val toMinimize: List[(String, String)] = elaborator.getPersone.asScala
      .filter(p => p.getPlainName.isPresent)
      .map(p => (p.getRegex, p.getPlainName.get())).toList

    // Salt for hashing
    val salt = scala.util.Random.nextInt()

    val processed: RDD[(String, Seq[EntryPoint], mutable.Map[String, Int], mutable.Map[String, String])] =
      sc.parallelize(inputs.toSeq.sortBy(_._1)) map {
      case (_, original, preprocessed, eps, unchangeable) =>
        // Map containing frequency of names
        val frequency: mutable.Map[String, Int] = mutable.Map()

        // Map containing identity associations
        val associations: mutable.Map[String, String] = mutable.Map()

        if (!preprocessed.isEmpty) {
          val modified = toMinimize.foldLeft(preprocessed)({ (minimized, personTuple) =>
            var tmp = minimized
            val (regex, plainName) = personTuple
            var matcher: Optional[Matcher] = AnonymUtils.search(regex, tmp, unchangeable.asJava)
            while (matcher.isPresent) {
              // Extract names as unique string from matcher
              val name: String = AnonymUtils.getAsUniqueFlatString(
                matcher.get().group.split("\\s+").toList.sorted.asJava
              )

              // Update frequency
              frequency.get(plainName) match {
                case None =>
                  frequency.put(plainName, 1)
                case Some(count) =>
                  frequency.update(plainName, count + 1)
              }

              // Update associations
              val id = associations.get(name) match {
                case None =>
                  // Calculate id
                  val id = AnonymUtils.getUniqueId(name, salt)
                  associations.put(name, id)
                  id
                case Some(id: String) =>
                  // Return already computed id
                  id
              }

              tmp = AnonymUtils.anonymize(tmp, eps.asJava, unchangeable.asJava, matcher.get(), id)
              matcher = AnonymUtils.search(regex, tmp, unchangeable.asJava)
            }
            tmp
          })
          (modified, eps, frequency, associations)
        } else
          (original, eps, frequency, associations)
    }

    println("The docx has been anonymized")

    val str_processed = processed.collect().map(_._1).toList.asJava
    val eps_processed = processed.collect().flatMap(_._2).toList.asJava
    AnonymUtils.postprocess(elaborator, str_processed, eps_processed)

    // Collect name frequencies
    val globalFreq: Map[String, Int] = processed.collect().flatMap(_._3.toList).groupBy(_._1) map {
      case (key: String, values: Array[(String, Int)]) =>
        key -> values.map(_._2).sum
    }

    // Collect associations
    val globalAssociations: Map[String, String] = processed.collect().map(_._4).reduce(_++_).toMap

    writeSeqToFile[Int](globalFreq.toSeq.sortBy(_._2)(Ordering[Int].reverse), "frequencies.txt", "FREQUENCIES:")
    writeSeqToFile[String](globalAssociations.toSeq.sortBy(_._1), "associations.txt", "ASSOCIATIONS:")
  }

  def writeSeqToFile[T](seqToWrite: Seq[(String, T)], fileName: String, header: String): Unit = {
    val file = new File(fileName)
    val writer = new PrintWriter(file)
    writer.println(header)
    seqToWrite foreach {
      case (key, value) =>
        writer.println(s"Key: $key\tValue: $value")
    }
    writer.close()
  }
}