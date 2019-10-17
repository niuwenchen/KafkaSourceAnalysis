package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Logging}

object Kafka  extends  Logging{
  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    if (args.length==0){
      CommandLineUtils.printUsageAndDie(optionParser,"USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

  }


}
