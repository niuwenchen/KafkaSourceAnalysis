package kafka.utils


import java.util.Properties

import joptsimple.{OptionParser, OptionSet, OptionSpec}

import scala.collection.Set




object CommandLineUtils extends  Logging {
  def isPrintHelpNeeded(commandOpts: CommandDefaultOptions): Boolean = {
    return commandOpts.args.length == 0 || commandOpts.options.has(commandOpts.helpOpt)
  }

  def printHelpAndExitIfNeeded(commandOpts: CommandDefaultOptions, message: String) = {
    if (isPrintHelpNeeded(commandOpts))
      printUsageAndDie(commandOpts.parser, message)
  }

  def checkRequiredArgs(parser:OptionParser,options:OptionSet,required:OptionSpec[_]*): Unit ={
    for (arg <- required){
      if (!options.has(arg))
        printUsageAndDie(parser,"Missing required argument \"" +arg+ "\"")
    }
  }

  def checkInvalidArgs(parser: OptionParser, options: OptionSet, usedOption: OptionSpec[_], invalidOptions: Set[OptionSpec[_]]) {
    if (options.has(usedOption)) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          printUsageAndDie(parser, "Option \"" + usedOption + "\" can't be used with option \"" + arg + "\"")
      }
    }
  }

  def checkInvalidArgsSet(parser: OptionParser, options: OptionSet, usedOptions: Set[OptionSpec[_]], invalidOptions: Set[OptionSpec[_]]) {
    if (usedOptions.count(options.has) == usedOptions.size) {
      for (arg <- invalidOptions) {
        if (options.has(arg))
          printUsageAndDie(parser, "Option combination \"" + usedOptions.mkString(",") + "\" can't be used with option \"" + arg + "\"")
      }
    }
  }

  /**
   * Parse key-value pairs in the form key=value
   * value may contain equals sign
   */
  def parseKeyValueArgs(args: Iterable[String], acceptMissingValue: Boolean = true): Properties = {
    val splits = args.map(_.split("=", 2)).filterNot(_.length == 0)

    val props = new Properties
    for (a <- splits) {
      if (a.length == 1 || (a.length == 2 && a(1).isEmpty())) {
        if (acceptMissingValue) props.put(a(0), "")
        else throw new IllegalArgumentException(s"Missing value for key ${a(0)}")
      }
      else props.put(a(0), a(1))
    }
    props
  }


  /**
   * Merge the options into {@code props} for key {@code key}, with the following precedence, from high to low:
   * 1) if {@code spec} is specified on {@code options} explicitly, use the value;
   * 2) if {@code props} already has {@code key} set, keep it;
   * 3) otherwise, use the default value of {@code spec}.
   * A {@code null} value means to remove {@code key} from the {@code props}.
   */
  def maybeMergeOptions[V](props: Properties, key: String, options: OptionSet, spec: OptionSpec[V]) {
    if (options.has(spec) || !props.containsKey(key)) {
      val value = options.valueOf(spec)
      if (value == null)
        props.remove(key)
      else
        props.put(key, value.toString)
    }
  }




  def printUsageAndDie(parser: OptionParser, str: String):Nothing ={
    System.err.println(str)
    parser.printHelpOn(System.err)
    Exit.exit(1,Some(str))
  }


}
