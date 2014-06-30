package util

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object TimeUtil {

	val second: Long = 1000L
	val minute: Long = 60 * second
	val hour: Long = 60 * minute
	val day: Long = 24 * hour

	private val compactDateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
	private val compactTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
	private val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
	private val timeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

	def parseDate(text: String): Long = {
		dateFormatter.parseMillis(text)
	}
	def parseCompactDate(text: String): Long = {
		compactDateFormatter.parseMillis(text)
	}
	def parseTime(text: String): Long = {
		timeFormatter.parseMillis(text)
	}
	def parseCompactTime(text: String): Long = {
		compactTimeFormatter.parseMillis(text)
	}

	def formatDate(millis: Long): String = {
		dateFormatter.print(millis)
	}
	def formatCompactDate(millis: Long): String = {
		compactDateFormatter.print(millis)
	}
	def formatTime(millis: Long): String = {
		timeFormatter.print(millis)
	}
	def formatCompactTime(millis: Long): String = {
		compactTimeFormatter.print(millis)
	}

}
