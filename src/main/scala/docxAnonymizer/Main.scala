package docxAnonymizer

import Control.using
import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.regex.Pattern

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{AmazonS3Exception, S3Object, S3ObjectInputStream}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.cli.{BasicParser, CommandLine, MissingArgumentException, Options}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.docx4j.openpackaging.exceptions.Docx4JException
import org.docx4j.openpackaging.packages.WordprocessingMLPackage
import org.docx4j.openpackaging.parts.WordprocessingML.{CommentsPart, EndnotesPart, FooterPart, FootnotesPart, HeaderPart}
import org.docx4j.openpackaging.parts.relationships.Namespaces

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import Function.tupled

object Main {
  // Flags
  var debug: Boolean = _
  var parallel: Boolean = _
  val log4jPrints: Boolean = false

  // CLI options
  private var inputFilePath: String = _
  private var outputFilePath: String = _
  private var s3Bucket: Option[String] = _
  private var minimizeFilePath: Option[String] = _
  private var keepUnchangedNamesFilePath: Option[String] = _
  private var keepUnchangedExprFilePath: Option[String] = _
  private var cmdArgsNames: Array[String] = _

  // Lists of people to minimize and not to minimize, respectively
  private var peopleToMinimize: List[Persona] = _
  private var peopleToKeepUnchanged: List[Persona] = _

  // AWS S3 credentials and client object
  private var credentials: BasicAWSCredentials = _
  private var s3Client: Option[AmazonS3] = _

  // Spark context
  var sparkContext: Option[SparkContext] = _

  // Config file
  val IAM_PATH = "config/iam_credentials.txt"

  private def disableDocxWarning(): Unit = {
    System.err.close()
    System.setErr(System.out)
  }

  private def cmdArgsRetriever(args: Array[String]): Either[String, Unit]  = {
    val cli_options: Options = new Options()
    cli_options.addOption("i", "input-file", true,
      "The docx input file to anonymize")
    cli_options.addOption("o", "output-file", true,
      "The docx output file generated")
    cli_options.addOption("s3", "s3bucket", true,
      "The S3 bucket in which files are stored")
    cli_options.addOption("m", "minimize", true,
      "The file with names and surnames to minimize. It must contain one expression per line. Names are separated by ':' between them and by ';' from the surname")
    cli_options.addOption("kn", "keep-names", true,
      "The file with names and surnames to keep unchanged (no minimization). It must contain one expression per line. Names are separated by ':' between them and by ';' from the surname")
    cli_options.addOption("ke", "keep-expressions", true,
      "The file with those expressions to be kept unchanged")
    cli_options.addOption("p", "parallel", false,
      "set parallel execution mode")
    cli_options.addOption("d", "debug", false,
      "set debug mode")
    val parser = new BasicParser

    // Flags
    Try(parser.parse(cli_options, args)) match {
      case Failure(ex: MissingArgumentException) => Left(ex.getMessage)
      case Success(commandLine: CommandLine) =>
        // Debug flag
        debug = commandLine.hasOption('d')
        if (debug) println("Debug mode: ON")
        // Parallel flag
        parallel = commandLine.hasOption('p')

        // Input file
        Option(commandLine.getOptionValue('i')) match {
          case None => Left("Path to input file is mandatory.")
          case Some(filePath: String) =>
            if (debug) {
              print("Input file: ")
              println(filePath)
            }
            // Check if file has the correct extension
            if (!filePath.endsWith(".docx")) Left("Input file must have '.docx' extension.")
            else {
              inputFilePath = filePath

              // Other optional files
              minimizeFilePath = Option(commandLine.getOptionValue("m"))
              keepUnchangedExprFilePath = Option(commandLine.getOptionValue("ke"))
              keepUnchangedNamesFilePath = Option(commandLine.getOptionValue("kn"))

              // AWS integration
              s3Bucket = Option(commandLine.getOptionValue("s3"))
              s3Bucket match {
                case None =>
                  s3Client = None
                  // Check local files' existence
                  if (!Files.exists(Paths.get(inputFilePath)))
                    return Left(s"Input file '$inputFilePath' not found")
                  if (minimizeFilePath.isDefined && !Files.exists(Paths.get(minimizeFilePath.orNull)))
                    return Left(s"Minimize file '${minimizeFilePath.orNull}' not found")
                  if (keepUnchangedNamesFilePath.isDefined && !Files.exists(Paths.get(keepUnchangedNamesFilePath.orNull)))
                    return Left(s"keepUnchangedNames file '${keepUnchangedNamesFilePath.orNull}' not found")
                  if (keepUnchangedExprFilePath.isDefined && !Files.exists(Paths.get(keepUnchangedExprFilePath.orNull)))
                    return Left(s"keepUnchangedExpr file '${keepUnchangedExprFilePath.orNull}' not found")
                case Some(bucketName: String) =>
                  // Read credentials from config
                  val (awsAccessKey, awsSecretKey): (String, String) =
                    using(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(IAM_PATH))) {
                      source => source.getLines().toList.take(2)
                    } match {
                      case List(a: String, b: String) => (a, b)
                    }
                  credentials = new BasicAWSCredentials(
                    awsAccessKey,
                    awsSecretKey
                  )

                  // Connect to AWS
                  s3Client = Option(AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_2)
                    .build())
                  if (debug) println("Connected to AWS.")

                  // Input file
                  downloadFromS3(bucketName, inputFilePath) match {
                    case Left(msg: String) => return Left(msg)
                    case Right(_: Unit) =>
                      // Minimize file
                      minimizeFilePath match {
                        case None =>
                        case Some(filePath: String) =>
                          downloadFromS3(bucketName, filePath) match {
                            case Left(msg: String) => return Left(msg)
                            case Right(_: Unit) =>
                          }
                      }
                      // keepUnchangedNames file
                      keepUnchangedNamesFilePath match {
                        case None =>
                        case Some(filePath: String) =>
                          downloadFromS3(bucketName, filePath) match {
                            case Left(msg: String) => return Left(msg)
                            case Right(_: Unit) =>
                          }
                      }
                      // keepUnchangedExpr file
                      keepUnchangedExprFilePath match {
                        case None =>
                        case Some(filePath: String) =>
                          downloadFromS3(bucketName, filePath) match {
                            case Left(msg: String) => return Left(msg)
                            case Right(_: Unit) =>
                          }
                      }
                  }
              }
              // Output file
              val defaultOutputFilePath: String = inputFilePath.replaceAll("\\.docx$", "-result\\.docx")
              outputFilePath = commandLine.getOptionValue('o', defaultOutputFilePath)
              if (debug) print(s"Output file: $outputFilePath")

              // Names passed as command line arguments
              cmdArgsNames = commandLine.getArgs

              // Check consistency of sources
              if (cmdArgsNames.length > 0 && (minimizeFilePath.isDefined || keepUnchangedNamesFilePath.isDefined))
                Left("Names must be specified via file or via args, not both")
              else Right()
            }
        }
    }
  }

  private def downloadFromS3(bucketName: String, filePath: String): Either[String, Unit] = {
    Try(s3Client.orNull.getObject(bucketName, filePath)) match {
      case Failure(ex: AmazonS3Exception) => Left(ex.getMessage)
      case Failure(ex: IllegalArgumentException) => Left("file not found on S3 bucket")
      case Success(s3Object: S3Object) =>
        val inputStream: S3ObjectInputStream = s3Object.getObjectContent
        val file = new File(filePath)
        Option(file.getParent) match {
          case None =>
          case Some(parentDirPath: String) =>
            new File(parentDirPath).mkdirs()
        }
        FileUtils.copyInputStreamToFile(inputStream, file)
        Right()
    }
  }

  private def preprocessing(): Either[String, Unit] = {
    // Read names to minimize from cmd or from minimizeFile
    readNamesFromFile(minimizeFilePath, (name: String) => name.charAt(0) == '!',
      "Number of people whose data must be minimized",
      "Automatic recognition of names", incrementalId = true) match {
      case Left(msg: String) => Left(msg)
      case Right(people: List[Persona]) =>
        peopleToMinimize = people

        // Read names to keep unchanged from cmd or from keepUnchangedNamesFile
        readNamesFromFile(keepUnchangedNamesFilePath, (name: String) => name.charAt(0) == '!',
          "Number of people whose data must not be minimized",
          "All data detected will be minimized", incrementalId = false) match {
          case Left(msg: String) => Left(msg)
          case Right(people: List[Persona]) =>
            peopleToKeepUnchanged = people

            Right()
        }
    }
  }

  private def readNamesFromFile(filePath: Option[String], filterFn: String => Boolean,
                                thenMsg: String, elseMsg: String,
                                incrementalId: Boolean): Either[String, List[Persona]] = {
    val toProcess: List[String] =
      if (cmdArgsNames.length > 0)
        cmdArgsNames.toList.filter(filterFn)
      else {
        filePath match {
          case None => List.empty[String]
          case Some(filePath: String) =>
            using(Source.fromFile(filePath)) {
              source => source.getLines().toList
            }
        }
      }
    if (debug) {
      if (toProcess.nonEmpty) println(s"$thenMsg: ${toProcess.length}")
      else println(elseMsg)
    }

    // Produce list of 'Persona' objects for the identities to minimize or to keep unchanged
    val (wellFormedNames, notWellFormedNames) =
      toProcess.partition(arg => arg.matches(Persona.NOMINATIVO_USER))
    if (notWellFormedNames.nonEmpty)
      Left(s"input arguments {${notWellFormedNames.mkString(", ")}} are not well-formed.")
    else {
      val (compatibleNames, tooLongNames): (List[(Array[String], String)], List[(Array[String], String)]) =
        wellFormedNames.map(arg => (
          arg.split(Pattern.quote(";"))(0).split(Pattern.quote(":")),
          arg.split(Pattern.quote(";"))(1))
        ).partition(tupled({
          (names, _) => names.length <= 10
        }))
      if (tooLongNames.nonEmpty)
        println(s"WARNING: for each person, only 10 names can be saved: data of the person with surnames {${tooLongNames.mkString(", ")}} will be ignored.")
      if (incrementalId) Right(
        for {
          ((names: Array[String], surname: String), id: Int) <- compatibleNames zip (1 to compatibleNames.length)
        } yield new Persona(surname, names.toList.asJava, id)
      )
      else Right(
        compatibleNames.map(tupled({
          (names, surname) => new Persona(surname, names.toList.asJava, -1)
        }))
      )
    }
  }

  private def docxProcessing(): Either[String, Unit] = {
    Try(WordprocessingMLPackage.load(new File(inputFilePath))) match {
      case Failure(_: Docx4JException) => Left(s"could not load $inputFilePath")
      case Success(wordMLPackage: WordprocessingMLPackage) =>
        val mainDocumentPart = wordMLPackage.getMainDocumentPart
        val rp = wordMLPackage.getMainDocumentPart.getRelationshipsPart
        val runNodesXPath = "//w:r"
        val runNodes: util.List[AnyRef] = mainDocumentPart.getJAXBNodesViaXPath(runNodesXPath, true)
        var tmp_runs: Option[util.List[AnyRef]] = None
        // Read nodes from document
        for (r <- rp.getRelationships.getRelationship.asScala) {
          r.getType match {
            case Namespaces.HEADER =>
              tmp_runs = Option(rp.getPart(r).asInstanceOf[HeaderPart].getJAXBNodesViaXPath(runNodesXPath, true))
            case Namespaces.FOOTER =>
              tmp_runs = Option(rp.getPart(r).asInstanceOf[FooterPart].getJAXBNodesViaXPath(runNodesXPath, true))
            case Namespaces.ENDNOTES =>
              tmp_runs = Option(rp.getPart(r).asInstanceOf[EndnotesPart].getJAXBNodesViaXPath(runNodesXPath, true))
            case Namespaces.FOOTNOTES =>
              tmp_runs = Option(rp.getPart(r).asInstanceOf[FootnotesPart].getJAXBNodesViaXPath(runNodesXPath, true))
            case Namespaces.COMMENTS =>
              tmp_runs = Option(rp.getPart(r).asInstanceOf[CommentsPart].getJAXBNodesViaXPath(runNodesXPath, true))
            case _ =>
              tmp_runs = None
          }
          // Unify data structures, distinct semantic blocks are then separated with a bottom-up approach
          if (tmp_runs.isDefined) {
            for (t <- tmp_runs.orNull.asScala) {
              runNodes.add(t)
            }
          }
        }

        // sparkContext.isDefined &&
        val worker: Worker = if (parallel) {
          new ParallelWorker(runNodes, peopleToMinimize, peopleToKeepUnchanged,
            keepUnchangedExprFilePath, debug, sparkContext.orNull)
        } else {
          new SequentialWorker(runNodes, peopleToMinimize.asJava, peopleToKeepUnchanged.asJava,
            keepUnchangedExprFilePath.orNull, debug)
        }

        // Anonymization
        worker.work()

        // Save output file
        val exportFile = new File(outputFilePath)
        Try(wordMLPackage.save(exportFile)) match {
          case Failure(_: Docx4JException) => Left(s"could not save $outputFilePath")
          case Success(_) =>
            s3Bucket match {
              case None => if (debug) println(s"Success! Output file: $outputFilePath")
              case Some(bucketName: String) =>
                Try(s3Client.orNull.putObject(bucketName, outputFilePath, exportFile)) match {
                  case Failure(_: AmazonS3Exception) => return Left("could not upload file to S3 bucket")
                  case Success(_) =>
                    if (debug) println(s"Success! Output file: $outputFilePath uploaded to S3 bucket")
                }
                val freqFile = new File("frequencies.txt")
                Try(s3Client.orNull.putObject(bucketName, Paths.get(exportFile.getParent, "frequencies.txt").toString, freqFile)) match {
                  case Failure(_: AmazonS3Exception) => return Left("could not upload file to S3 bucket")
                  case Success(_) =>
                    if (debug) println("Success! Output file: 'frequencies.txt' uploaded to S3 bucket")
                }
                val assocFile = new File("associations.txt")
                Try(s3Client.orNull.putObject(bucketName, Paths.get(exportFile.getParent, "associations.txt").toString, assocFile)) match {
                  case Failure(_: AmazonS3Exception) => return Left("could not upload file to S3 bucket")
                  case Success(_) =>
                    if (debug) println("Success! Output file: 'associations.txt' uploaded to S3 bucket")
                }
            }
            Right()
        }
    }
  }

  def main(args: Array[String]): Unit = {
    sparkContext = try {
      Option(new SparkContext(new SparkConf().setAppName("DocxAnon")))
    } catch {
      case _: NoClassDefFoundError =>
        println("Spark platform not found, running locally")
        None
    }

    // Logger configuration
    BasicConfigurator.configure()
    if (log4jPrints) {
      Logger.getRootLogger.setLevel(Level.OFF)
      disableDocxWarning()
    }

    // Retrieve command line arguments
    cmdArgsRetriever(args) match {
      case Left(message: String) => println(s"ERROR: $message")
      case Right(_: Unit) =>
        // Preprocess document
        preprocessing() match {
          case Left(message: String) => println(s"ERROR: $message")
          case Right(_: Unit) =>
            // Docx elaboration
            docxProcessing()
        }
    }

    sparkContext match {
      case Some(sparkContext: SparkContext) =>
        sparkContext.stop()
      case None =>
    }
    println("Terminating...")
  }
}
