# DocxAnonymizer-spark
Stand-alone Scala &amp; Java tool to anonymize OOXML Documents (docx). This software helps to make docx documents compliant to [General Data Protection Regulation 2016/679 (**GDPR**)](https://eur-lex.europa.eu/legal-content/IT/TXT/?uri=uriserv:OJ.L_.2016.119.01.0001.01.ITA&toc=OJ:L:2016:119:TOC).

This project is an extension of [DocxAnonymizer-core](https://github.com/Lostefra/DocxAnonymizer-core). Both software perform the same tasks, but [DocxAnonymizer-core](https://github.com/Lostefra/DocxAnonymizer-core) can work only on a single machine, whereas [DocxAnonymizer-spark](https://github.com/Lostefra/DocxAnonymizer-spark) can work either locally or on a cluster with **Apache Spark** (e.g. on [Amazon EMR](https://aws.amazon.com/emr/?nc1=h_ls)). The latter implementation is completely scalable and drastically more efficient especially in processing large documents.

This project was developed as part of the *Languages and Algorithms for Artificial Intelligence* university course (Master in Artificial Intelligence, Alma Mater Studiorum - University of Bologna).

## Authors

* [Lorenzo Mario Amorosa](https://github.com/Lostefra)
* [Mattia Orlandi](https://github.com/nihil21)
* [Giacomo Pinardi](https://github.com/GiacomoPinardi)

## Workflow

Given a **complex** and **rich-formatted docx** file, the program extrapolates all the text, it **anonymizes** its content and then it saves the new docx **without altering its structure**.

**Nominatives** (i.e. sequences of names and surnames) are **replaced** with anonymous **IDs** and multiple occurrences of the same nominatives are replaced with the same ID. 

The **detection** of the nominatives can be **either on demand or automatic**. In fact, the user can express as input the sequences of names-surname to anonymize; in case these sequences are not given, the program automatically starts searching for nominatives in the document using dictionaries of Italian and English names. A pattern-based approach is adopted to detect nominatives. [Further details here](https://github.com/Lostefra/DocxAnonymizer-core/blob/master/docs/TESI_Lorenzo_Mario_Amorosa.pdf).

In brief, the [program](https://github.com/Lostefra/DocxAnonymizer-spark/blob/func_style/src/main/scala/docxAnonymizer/Main.scala) accepts the following options:
```sh
-i  <input-file>       [the docx input file to anonymize]
-o  <output-file>      [the docx output file generated, if not expressed given by default]
-s3 <s3bucket>         [the S3 bucket in which files are stored, all file paths are relative to the bucket. If s3bucket is not provided, files are supposed to be stored locally] 
-m  <minimize>         [the file with names and surnames to minimize. It must contain one expression per line of the form: "<name1>:<name2>:[...]:<nameN>;<surname>", if not expressed the program will perform automatic detection of nominatives]
-kn <keep-names>       [the file with names and surnames to keep unchanged (no minimization). It must contain one expression per line of the form: "<name1>:<name2>:[...]:<nameN>;<surname>"]
-ke <keep-expressions> [the file with those expressions to be kept unchanged (not nominatives)]
-p  <parallel>         [enable distributed execution]
-d  <debug>            [increase verbosity]
```
  
## Notes

DocxAnonymizer leverage the [docx4j](https://www.docx4java.org/trac/docx4j) library.

-===============================================================
CANCELLARE SOTTO
-===============================================================

Possibili sviluppi progetto Scala: Docx Anonymizer.

 - Cruciale: architettura distribuita per il processamento dei documenti -

1) Dati N server, M nomi del dizionario: si da' a ogni server la responsabilita di processare solo M/N nomi del dizionario. [HARD]
   - Variante correlata: se N >> 1, si puo' fare load balancing nella distribuzioni di documenti/ porzioni di un singolo documento [HARD]
2) Implementare i dizionari con database (non file) in uno scenario di processamento distribuito dei dati: 
   - tenere conto del numero di occorrenze dei nomi nei documenti per ottimizzazione: saranno processati prima i nodi piu' frequenti [MEDIUM]
   - ampliamento dinamico del dizionario con nuovi nomi mediante uno schema a "candidatura" [MEDIUM]
3) Ampliare il parco di dati personali da minimizzare (date e luoghi di nascita, codici fiscali, indirizzi, email, numeri di telefono, sesso, dati documenti di identita', etc.). Nota: non tutte le date e non tutte le citta vanno minimizzate [EASY]

 - Da decidere: refactor in scala di quali parti del codice? -

N SERVER:
- 1 documento => suddiviso in N parti;

== Ipotesi 1 ==
- Ogni server ha i suoi nomi; processa e minimizza una parte di documento; scambia la propria parte con un altro server che non ha ancora processato quella porzione di documento

== Ipotesi 2 ==
- Tutti i nominativi vengono individuati in un primo momento dai server, successivamente in parallelo gli N server minimizzano tenendo conto di tutti nominativi individuati

