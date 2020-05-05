package docxAnonymizer

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.util
import java.util.regex.Pattern

import javax.xml.bind.JAXBException
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.docx4j.jaxb.XPathBinderAssociationIsPartialException
import org.docx4j.openpackaging.exceptions.Docx4JException
import org.docx4j.openpackaging.packages.WordprocessingMLPackage
import org.docx4j.openpackaging.parts.WordprocessingML.CommentsPart
import org.docx4j.openpackaging.parts.WordprocessingML.EndnotesPart
import org.docx4j.openpackaging.parts.WordprocessingML.FooterPart
import org.docx4j.openpackaging.parts.WordprocessingML.FootnotesPart
import org.docx4j.openpackaging.parts.WordprocessingML.HeaderPart
import org.docx4j.openpackaging.parts.relationships.Namespaces

import scala.jdk.CollectionConverters._

/**
 *
 * @author lorenzo
 *
 *         Classe contenente il Main di Docx Anonymizer.
 *
 *         Parametri di invocazione:
 *         -p               [Facoltativo, se specificato si esegue la minimizzazione in parallelo su cluster]
 *         -i <inputFile>   [Obbligatorio, file Docx del quale si vogliono minimizzare i dati contenuti]
 *         -o <outputFile>  [Facoltativo, file Docx prodotto da Docx Anonymizer; se non espresso impostato a default]
 *         -m <minimize>    [Facoltativo, file contenente riga per riga i nominativi da minimizzare, nella forma: "<nome1>:<nome2>:[...]:<nomeN>;<cognome>"]
 *         [  Nota bene: qualora il file fosse assente Docx Anonymizer impieghera' i dizionari nella ricerca dei nominativi               ]
 *         -kn <keepNames>  [Facoltativo, file contenente riga per riga i nominativi da NON minimizzare, nella forma di cui sopra]
 *         -ke <keepExpr>   [Facoltativo, file contenente riga per riga delle espressioni da NON minimizzare (non nominativi)]
 *         [  Nota bene: alcune espressioni sono gia' impostate a default in config/keep_unchanged.txt      ]
 *         -d <debug>       [Facoltativo, se tale opzione e' impostata sono stampate a video informazioni sull'esecuzione]
 *
 *         In alternativa, i nominativi da minimizzare possono essere forniti direttamente da linea di comando nello stesso formato indicato precedentemente
 *         Cio' e' possibile anche per i nominativi da NON minimizzare, andra' in tal caso anteposto al primo nome un punto esclamativo '!'
 *
 */
object Main extends App {
  //debug=true attiva alcune stampe utili
  var debug:Boolean = false
  // parallel=true attiva esecuzione parallela
  var parallel:Boolean = false
  //log4jPrints=false disattiva stampe di routine usate da docx4j
  val log4jPrints:Boolean = false
  private var inputFile:String = _
  private var outputFile:String = _
  private var minimizeFile:String = _
  private var keepUnchangedNamesFile:String = _
  private var keepUnchangedExprFile:String = _
  //elenco persone di cui minimizzare i dati
  private val persone:util.ArrayList[Persona] = new util.ArrayList[Persona]
  //persone di cui NON minimizzare i dati
  private val keepUnchanged:util.ArrayList[Persona] = new util.ArrayList[Persona]

  /*
   * Per disabilitare il warning:
   *
   * WARNING: An illegal reflective access operation has occurred
     WARNING: Illegal reflective access by com.sun.xml.bind.v2.runtime.reflect.opt.Injector$1 (file:/home/lorenzo/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.7/jaxb-impl-2.2.7.jar) to method java.lang.ClassLoader.defineClass(java.lang.String,byte[],int,int)
     WARNING: Please consider reporting this to the maintainers of com.sun.xml.bind.v2.runtime.reflect.opt.Injector$1
     WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
     WARNING: All illegal access operations will be denied in a future release
   *
   */
  private def disableDocxWarning(): Unit = {
    System.err.close()
    System.setErr(System.out)
  }

  //check input and inizializzazione variabili
  private def parametersCheck(args: Array[String]): Unit = {
    val option_i = Option.builder("i")
      .required(true)
      .desc("The docx input file to anonymize")
      .longOpt("input-file")
      .hasArg.build
    val option_o = Option.builder("o")
      .required(false)
      .desc("The docx output file generated")
      .longOpt("output-file")
      .hasArg.build
    val option_m = Option.builder("m")
      .required(false)
      .desc("The file with names and surnames to minimize. "
        + "It must contain one expression per line. "
        + "Names are separated by ':' between them and by ';' from the surname")
      .longOpt("minimize")
      .hasArg.build
    val option_kn = Option.builder("kn")
      .required(false)
      .desc("The file with names and surnames to keep unchanged (no minimization). "
        + "It must contain one expression per line. "
        + "Names are separated by ':' between them and by ';' from the surname")
      .longOpt("keep-names")
      .hasArg.build
    val option_ke = Option.builder("ke")
      .required(false)
      .desc("The file with those expressions to be kept unchanged")
      .longOpt("keep-expressions")
      .hasArg.build
    val option_p = Option.builder("p")
      .required(false)
      .desc("set parallel execution mode")
      .longOpt("parallel")
      .build
    val option_d = Option.builder("d")
      .required(false)
      .desc("set debug mode")
      .longOpt("debug")
      .build
    val options = new Options
    val parser = new DefaultParser
    options.addOption(option_i)
    options.addOption(option_o)
    options.addOption(option_m)
    options.addOption(option_kn)
    options.addOption(option_ke)
    options.addOption(option_p)
    options.addOption(option_d)
    try {
      val commandLine = parser.parse(options, args)
      if (commandLine.hasOption('d')) {
        debug = true
        println("Modalita' debug: ON")
      }
      if (commandLine.hasOption('p')) parallel = true
      if (debug) {
        print("Input file: ")
        println(commandLine.getOptionValue("i"))
      }
      inputFile = commandLine.getOptionValue("i")
      if (!inputFile.endsWith(".docx")) {
        val pe = new ParseException("ERRORE: l'input file '" + inputFile + "' deve avere estensione \".docx\"")
        throw pe
      }
      /* attribuisco nome di default a docx generato in output se non specificato dall'utente */
      val defaultOutput = commandLine.getOptionValue("i").replaceAll("\\.docx$", "-result\\.docx")
      if (debug) {
        print("Output file: ")
        println(commandLine.getOptionValue("o", defaultOutput))
      }
      outputFile = commandLine.getOptionValue("o", defaultOutput)
      keepUnchangedExprFile = commandLine.getOptionValue("ke")
      /*
       * Verifico che tutti i nominativi passati come argomento siano ben formati:
       * - Possono essere presenti da 1 a 10 nomi ed 1 cognome obbligatoriamente
       * - I nomi sono separati tra loro da ':' e dal cognome da ';'
       * - I nomi possono contenere lettere minuscole e maiuscole (anche accentate) e l'apostrofo
       * - I cognomi contengono gli stessi caratteri dei nomi ed anche spazi bianchi e separatori non visibili
       *
       * Nota bene:
       *  - Un nominativo preceduto da "!" non deve essere minimizzato
       *
       * Esempi di nominativi validi:
       * - "Lorenzo;Amorosa"
       * - "Lorenzo:Mario;Amorosa"
       * - "Lorenzo:Mario;De Amorosa"
       * - "L'òrénzò;D'Amorosa sa sa"
       * - "!Lorenzo;Amorosa"
       *
       */
      // I nominativi sono o solo specificati via file o solo specificati via args
      if (commandLine.getArgs.length > 0 && (commandLine.hasOption('m') || commandLine.hasOption("kn"))) {
        val pe = new ParseException("ERRORE: i nominativi devono essere o solo specificati via file o solo specificati via args")
        throw pe
      }
      val nominativi = new util.ArrayList[String]
      if (commandLine.getArgs.length > 0) {
        for (argument:String <- util.Arrays.asList(commandLine.getArgs:_*).asScala){
          if(argument.charAt(0) != '!')
            nominativi.add(argument)
        }
      }
      else try {
        minimizeFile = commandLine.getOptionValue("m")
        if (minimizeFile != null) {
          var toMinimize:String = null
          val bf = new BufferedReader(new FileReader(minimizeFile))
          toMinimize = bf.readLine
          if (toMinimize != null) nominativi.add(toMinimize.trim)
          while (toMinimize != null) {
            toMinimize = bf.readLine
            if (toMinimize != null) nominativi.add(toMinimize.trim)
          }
          bf.close()
        }
      } catch {
        case e: IOException =>
          e.printStackTrace()
          System.exit(7)
      }
      if (debug) {
        println()
        if (nominativi.size > 0) println("Numero persone di cui minimizzare i dati: " + nominativi.size)
        else println("Riconoscimento automatico dei nominativi")
        println()
      }
      var cognome:String = null
      var nomiString:String = null
      var nomi:util.List[String] = null
      var id:Int = 1

      for (argument:String <- nominativi.asScala) {
        if (argument.matches(Persona.NOMINATIVO_USER)) {
          nomiString = argument.split(Pattern.quote(";"))(0)
          cognome = argument.split(Pattern.quote(";"))(1)
          nomi = util.List.of(nomiString.split(Pattern.quote(":")):_*)
          if (nomi.size > 10)
            println("WARNING: sono inseribili un massimo di 10 nomi per persona, non minimizzo i dati"
              + " della persona con cognome: " + cognome + ". Procedo con l'elaborazione.")
          else {
            //aggiungo la persona alla lista
            persone.add(new Persona(cognome, nomi, id))
            id += 1
          }
        }
        else {
          val pe = new ParseException(argument + ": input non ben formato")
          throw pe
        }
      }
      val keepUnchanged_string = new util.ArrayList[String]
      if (commandLine.getArgs.length > 0) {
        for (argument: String <- util.Arrays.asList(commandLine.getArgs:_*).asScala) {
          if (argument.charAt(0) == '!')
            keepUnchanged_string.add(argument)
        }
      }
      else try {
        keepUnchangedNamesFile = commandLine.getOptionValue("kn")
        if (keepUnchangedNamesFile != null) {
          var toKeep:String = null
          val bf = new BufferedReader(new FileReader(keepUnchangedNamesFile))
          toKeep = bf.readLine
          if (toKeep != null) keepUnchanged_string.add(toKeep.trim)
          while (toKeep != null) {
            toKeep = bf.readLine
            if (toKeep != null) keepUnchanged_string.add(toKeep.trim)
          }
          bf.close()
        }
      } catch {
        case e: IOException =>
          e.printStackTrace()
          System.exit(8)
      }
      if (debug) {
        if (keepUnchanged_string.size > 0) println("Numero persone di cui NON minimizzare i dati: " + keepUnchanged_string.size)
        else println("Minimizzazione di tutti i dati individuati")
        println()
      }
      id = -1
      for (argument <- keepUnchanged_string.asScala) {
        if (argument.matches(Persona.NOMINATIVO_USER)) {
          nomiString = argument.split(Pattern.quote(";"))(0).replaceAll("!", "")
          cognome = argument.split(Pattern.quote(";"))(1)
          nomi = util.List.of(nomiString.split(Pattern.quote(":")):_*)
          if (nomi.size > 10)
            println("WARNING: sono inseribili un massimo di 10 nomi per persona, non minimizzo i dati"
              + " della persona con cognome: " + cognome + ". Procedo con l'elaborazione.")
          else keepUnchanged.add(new Persona(cognome, nomi, id))
        }
        else {
          val pe = new ParseException(argument + ": input non ben formato")
          throw pe
        }
      }
    } catch {
      case exception: ParseException =>
        print("Parse error: ")
        println(exception.getMessage)
        System.exit(1)
    }

    if (debug) {
      if(parallel) println("Input validato, inizio processamento parallelo")
      else println("Input validato, inizio processamento sequenziale")
    }
  }

  /**********************/
  /*******  MAIN  *******/
  /**********************/

  /*
   * inizializzazione log4j
   * la variabile "log4jPrints" a false disabilita stampe verbose di logging inerenti ad apertura file .docx (di cui si occupa interamente docx4j)
   */
  BasicConfigurator.configure()
  if (!log4jPrints) {
    Logger.getRootLogger.setLevel(Level.OFF)
    disableDocxWarning()
  }
  parametersCheck(args)
  val doc = new File(inputFile)
  var wordMLPackage:WordprocessingMLPackage = _
  try {
    //carico il documento .docx
    wordMLPackage = WordprocessingMLPackage.load(doc)
  } catch {
    case e: Docx4JException =>
      println("Eccezione in WordprocessingMLPackage.load: caricamento fallito di " + inputFile)
      e.printStackTrace()
      System.exit(2)
  }
  //ottengo document.xml che contiene i dati di interesse
  val mainDocumentPart = wordMLPackage.getMainDocumentPart
  val rp = wordMLPackage.getMainDocumentPart.getRelationshipsPart
  // XPath: https://www.w3.org/TR/xpath-30/
  val runNodesXPath = "//w:r"
  var runNodes:util.List[AnyRef] = _
  var tmp_runs:util.List[AnyRef] = _
  try {
    //ottengo tutti i nodi contenenti il testo visibile nel documento
    // Esempi: https://github.com/plutext/docx4j/search?q=getJAXBNodesViaXPath+in%3Afile&type=Code
    runNodes = mainDocumentPart.getJAXBNodesViaXPath(runNodesXPath, true)
    //ottengo tutti i rimanenti nodi contenenti testo presenti in altri file xml
    for (r <- rp.getRelationships.getRelationship.asScala) {
      r.getType match {
        case Namespaces.HEADER =>
          tmp_runs = rp.getPart(r).asInstanceOf[HeaderPart].getJAXBNodesViaXPath(runNodesXPath, true)
        case Namespaces.FOOTER =>
          tmp_runs = rp.getPart(r).asInstanceOf[FooterPart].getJAXBNodesViaXPath(runNodesXPath, true)
        case Namespaces.ENDNOTES =>
          tmp_runs = rp.getPart(r).asInstanceOf[EndnotesPart].getJAXBNodesViaXPath(runNodesXPath, true)
        case Namespaces.FOOTNOTES =>
          tmp_runs = rp.getPart(r).asInstanceOf[FootnotesPart].getJAXBNodesViaXPath(runNodesXPath, true)
        case Namespaces.COMMENTS =>
          tmp_runs = rp.getPart(r).asInstanceOf[CommentsPart].getJAXBNodesViaXPath(runNodesXPath, true)
        case _ =>
          tmp_runs = null
      }
      // unifico le strutture dati, blocchi semantici distinti sono separati successivamente con approccio bottom-up
      if (tmp_runs != null) {
        for (t <- tmp_runs.asScala) {
          runNodes.add(t)
        }
      }
    }
  } catch {
    case e@(_: XPathBinderAssociationIsPartialException | _: JAXBException) =>
      println("Eccezione in mainDocumentPart.getJAXBNodesViaXPath, lettura fallita da " + inputFile)
      e.printStackTrace()
      System.exit(3)
  }
  // debug mode setting
  if(debug) {
    Persona.setDebug(debug)
    Elaborator.setDebug(debug)
  }
  //elaboro i nodi contenuti in document.xml e minimizzo i dati personali contenuti
  var elab:Elaborator = _
  if (keepUnchangedExprFile != null) elab = new Elaborator(runNodes, persone, keepUnchanged, keepUnchangedExprFile)
  else elab = new Elaborator(runNodes, persone, keepUnchanged)

  if(parallel) {
    val s_preMinimization = elab.preprocess()
    // TODO REMOVE DAI PARAMETRI DI INPUT "elab" => SERVE SOLO PER SPERIMENTAZIONE SPARK MOCK !!!!!
    val worker = new Worker(elab, s_preMinimization, elab.getPlainTexts(), elab.getPersone(), elab.getKeepUnchanged(), elab.getToKeepViaConfig())
    val s_postMinimization = worker.work
    // TODO DECOMMENTARE QUESTA RIGA, NON SERVE NELLA VERSIONE MOCK SPARK SEQUENZIALE, SERVE IN VERSIONE PARALLELA !!!
    //elab.postprocess(s_postMinimization)
  }
  else elab.work()

  //salvataggio nuovo file docx modificato
  val exportFile = new File(outputFile)
  try wordMLPackage.save(exportFile)
  catch {
    case e: Docx4JException =>
      println("Eccezione in wordMLPackage.save, salvataggio fallito di " + outputFile)
      e.printStackTrace()
      System.exit(4)
  }
  if (debug) println("Success! Output file: " + outputFile)

}