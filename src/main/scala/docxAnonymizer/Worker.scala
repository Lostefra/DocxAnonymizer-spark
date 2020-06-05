package docxAnonymizer

trait Worker {
  def work() : Unit
}

abstract class AbstractWorker extends Worker