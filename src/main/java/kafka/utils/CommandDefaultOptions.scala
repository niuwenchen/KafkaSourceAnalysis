package kafka.utils

import joptsimple.{OptionParser, OptionSet}

abstract  class CommandDefaultOptions(val args:Array[String],allowCommandOptionAbbreviation: Boolean = false) {
  val parser = new OptionParser(allowCommandOptionAbbreviation)
  val helpOpt = parser.accepts("help","Print usage information")
  var options: OptionSet =_
}
