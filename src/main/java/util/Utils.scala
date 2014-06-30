package util

import java.nio.charset.{CodingErrorAction, Charset}
import java.util.Random
import java.util.regex.{Pattern, Matcher}
import org.apache.commons.lang3.StringUtils.isEmpty
import org.apache.commons.lang3.math.NumberUtils.toInt
import scala.io.Codec

object Utils {

	def pHr() {
		println("------------------------------------------------------------")
	}

	/* ------------------------- string ------------------------- */

	def find(regex: Pattern, s: String): String = {
		val m: Matcher = regex.matcher(s)
		if (m.find) m.group(if (m.groupCount > 0) 1 else 0) else ""
	}
	def findInt(regex: Pattern, s: String): Int = {
		val i: String = find(regex, s)
		if (isEmpty(i)) -1 else toInt(i)
	}

	val utf8 = "UTF-8"
	val charsetUtf8 = Charset.forName(utf8)

	// bell(2014-3): 主要用于 Source.fromFile，避免异常字符打断流式处理
	// ref: http://stackoverflow.com/questions/7280956/how-to-skip-invalid-characters-in-stream-in-java-scala
	private val decoderUtf8 = charsetUtf8.newDecoder()
	decoderUtf8.onMalformedInput(CodingErrorAction.IGNORE)
	val codecUtf8 = Codec(decoderUtf8)

	/* ------------------------- misc ------------------------- */

	def sleep(millis: Long) {
		try {
			if (millis > 0) Thread.sleep(millis)
		}
		catch {
			case e: InterruptedException =>
		}
	}

	def random: Double = Math.random
	def randInt(n: Int): Int = rand.nextInt(n)
	private final val rand: Random = new Random

}
