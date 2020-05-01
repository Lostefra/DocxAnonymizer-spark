package docxAnonymizer

import java.util

import org.apache.spark.sql.SparkSession

class Worker(val s_preMinimization: util.List[java.lang.StringBuilder],
             val plainTexts: PlainTexts,
             val persone: util.List[Persona],
             val keepUnchanged: util.List[Persona],
             val keepViaConfig: String) {

    //TODO sparkSession etc. , verificare classe necessaria
    private val sparkSession = SparkSession.builder().getOrCreate()
    private val s_postMinimization = new util.ArrayList[java.lang.StringBuilder]

    def work() : util.List[java.lang.StringBuilder] = {
        //TODO WORK METHOD: alcune delle variabili necessarie sono state gia' passate al costruttore
      /*
    val tmp: String = null
    val curr_entryPoints: util.List[EntryPoint] = null
    val unchangeable: util.List[EntryPoint] = null
    // analizzo tutte le sotto-stringhe in cui ho raccolto i nodi Docx, filtrate dal pre-processamento
    var index: Int = 0

    for (s <- s_preMinimization)  { // se il testo deve essere minimizzato
    if (!(s.toString == ""))  { curr_entryPoints = plainTexts.getEntryPoints.stream.filter((x: EntryPoint) => x.getIndex_PlainText == plainTexts.getPlainTexts.indexOf(s)).collect(Collectors.toList)
    // rimuovo le occorrenze dei nominativi di ogni persona
    tmp = s.toString
    unchangeable = new ArrayList[EntryPoint]
    import scala.collection.JavaConversions._
    for (p <- keepUnchanged)  { plainTexts.markUnchangeableEntryPoints(unchangeable, p.getRegex, index)
    }
    updateUnchangeableViaConfig(unchangeable, index)
    import scala.collection.JavaConversions._
    for (p <- persone)  {
    //TODO: Persona.minimizza() sostituisce l'ID di quella persona al testo da minimizzare -> l'ID sara' nella collection scala condivisa (attualemente ID è un campo di Persona)
    //TODO: invece curr_entryPoints, unchangeable sono strutture dati locali riguardanti 1 solo StringBuilder
    //TODO nota: basta aggiungere una funzione "minimizza" che prende in input l'id da sostituire al nominativo
    tmp = p.minimizza(tmp, curr_entryPoints, unchangeable)
    }
    // inserisco la stringa minimizzata nella lista temporanea "s_postMinimization"
    s_postMinimization.add(new StringBuilder(tmp))
    }
    else  { // se il testo non deve essere minimizzato
    // inserisco la stringa che non necessitava di minimizzazione nella lista temporanea "s_postMinimization"
    s_postMinimization.add(plainTexts.getPlainTexts.get(index))
    }
    index += 1
    }


    */
        // return StringBuilder post-minimizzazione
        s_postMinimization

    }

}
