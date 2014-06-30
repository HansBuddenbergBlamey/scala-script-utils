package util

import java.sql._
import javax.sql.DataSource

import _root_.util.DBRunner.{ListHandler, SingleHandler}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.dbutils.{AbstractQueryRunner, DbUtils, ResultSetHandler}

import scala.collection.mutable.ListBuffer

object DBRunner {

	// private val logger: Logger = LoggerFactory.getLogger(classOf[DBRunner])

	private def getDataSource(host: String, port: Int, dbName: String, user: String, pass: String, maxConn: Int, timeout: Long): DataSource = {
		val config = new HikariConfig
		config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource")
		config.addDataSourceProperty("url", s"jdbc:mysql://$host:$port/$dbName?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=utf-8")
		config.addDataSourceProperty("user", user)
		config.addDataSourceProperty("password", pass)
		config.addDataSourceProperty("cachePrepStmts", "true")
		config.addDataSourceProperty("prepStmtCacheSize", "250")
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
		config.addDataSourceProperty("useServerPrepStmts", "true")
		config.setMaximumPoolSize(maxConn)
		config.setMinimumPoolSize(0)
		config.setConnectionTimeout(timeout)
		config.setMaxLifetime(timeout)
		new HikariDataSource(config)
	}

	class SingleHandler[T](default: T, mapper: ResultSet => T) extends ResultSetHandler[T] {
		override def handle(rs: ResultSet): T = {
			if (rs.next) mapper.apply(rs) else default
		}
	}

	class ListHandler[T](mapper: ResultSet => T) extends ResultSetHandler[List[T]] {
		override def handle(rs: ResultSet): List[T] = {
			val rows = new ListBuffer[T]
			while (rs.next)
				rows += mapper(rs)
			rows.toList
		}
	}

	class DumpHandler extends ResultSetHandler[Int] {
		override def handle(rs: ResultSet): Int = {
			val meta = rs.getMetaData
			val len = meta.getColumnCount

			var rows = 0
			while (rs.next()) {
				// bell(2014-6): 无输出则不print thead
				if (rows == 0) {
					(1 to len)
						.map(meta.getColumnLabel)
						.foreach(obj => print(obj + " \t "))
					println()
				}

				rows += 1
				(1 to len)
					.map(rs.getObject)
					.foreach(obj => print(obj + " \t "))
				println()
			}
			rows
		}
	}

	private final val singleIntHandler = new SingleHandler[Int](0, _.getInt(1))
	private final val singleLongHandler = new SingleHandler[Long](0, _.getLong(1))
	private final val singleDoubleHandler = new SingleHandler[Double](0, _.getDouble(1))
	private final val singleStringHandler = new SingleHandler[String]("", _.getString(1))
	private final val singleTimestampHandler = new SingleHandler[Timestamp](null, _.getTimestamp(1))
	private final val singleBytesHandler = new SingleHandler[scala.Array[Byte]](null, _.getBytes(1))

	private final val intListHandler = new ListHandler[Int](_.getInt(1))
	private final val longListHandler = new ListHandler[Long](_.getLong(1))
	private final val strListHandler = new ListHandler[String](_.getString(1))
	private final val strDoubleListHandler = new ListHandler[(String, Double)](rs => (rs.getString(1), rs.getDouble(2)))
	private final val intDoubleListHandler = new ListHandler[(Int, Double)](rs => (rs.getInt(1), rs.getDouble(2)))
	private final val intIntListHandler = new ListHandler[(Int, Int)](rs => (rs.getInt(1), rs.getInt(2)))
	private final val strStrDoubleListHandler = new ListHandler[(String, String, Double)](rs => (rs.getString(1), rs.getString(2), rs.getDouble(3)))

	private final val dumpHandler = new DumpHandler()

}

class DBRunner(ds: DataSource) extends AbstractQueryRunner(ds: DataSource) {

	def this(host: String, port: Int, dbName: String, user: String, pass: String, maxConn: Int, timeout: Long) {
		this(DBRunner.getDataSource(host, port, dbName, user, pass, maxConn, timeout))
	}
	def this(host: String, dbName: String, user: String, pass: String, maxConn: Int) {
		this(host, 3306, dbName, user, pass, maxConn, 20 * TimeUtil.minute)
	}
	def this(host: String, dbName: String, user: String, pass: String) {
		this(host, dbName, user, pass, 1000)
	}

	/* ------------------------- methods ------------------------- */

	// bell(2014-3): scala Any* -> java Object...
	// http://stackoverflow.com/questions/2334200/transforming-scala-varargs-into-java-object-varargs

	def queryInt(sql: String, params: Any*): Int = {
		query(sql, DBRunner.singleIntHandler, params: _*)
	}
	def queryLong(sql: String, params: Any*): Long = {
		query(sql, DBRunner.singleLongHandler, params: _*)
	}
	def queryDouble(sql: String, params: Any*): Double = {
		query(sql, DBRunner.singleDoubleHandler, params: _*)
	}
	def queryString(sql: String, params: Any*): String = {
		query(sql, DBRunner.singleStringHandler, params: _*)
	}
	def queryTimestamp(sql: String, params: Any*): Timestamp = {
		query(sql, DBRunner.singleTimestampHandler, params: _*)
	}
	def queryBytes(sql: String, params: Any*): scala.Array[Byte] = {
		query(sql, DBRunner.singleBytesHandler, params: _*)
	}

	def queryIntList(sql: String, params: Any*): List[Int] = {
		query(sql, DBRunner.intListHandler, params: _*)
	}
	def queryLongList(sql: String, params: Any*): List[Long] = {
		query(sql, DBRunner.longListHandler, params: _*)
	}
	def queryStringList(sql: String, params: Any*): List[String] = {
		query(sql, DBRunner.strListHandler, params: _*)
	}

	def queryStrDblList(sql: String, params: Any*): List[(String, Double)] = {
		query(sql, DBRunner.strDoubleListHandler, params: _*)
	}
	def queryIntDblList(sql: String, params: Any*): List[(Int, Double)] = {
		query(sql, DBRunner.intDoubleListHandler, params: _*)
	}
	def queryIntIntList(sql: String, params: Any*): List[(Int, Int)] = {
		query(sql, DBRunner.intIntListHandler, params: _*)
	}
	def queryStrStrDblList(sql: String, params: Any*): List[(String, String, Double)] = {
		query(sql, DBRunner.strStrDoubleListHandler, params: _*)
	}

	def queryOne[T](sql: String, mapper: ResultSet => T, params: Any*): T = {
		queryOne[T](sql, null.asInstanceOf[T], mapper, params: _*)
	}
	def queryOne[T](sql: String, default: T, mapper: ResultSet => T, params: Any*): T = {
		query(sql, new SingleHandler[T](default, mapper), params: _*)
	}
	def queryList[T](sql: String, mapper: ResultSet => T, params: Any*): List[T] = {
		query(sql, new ListHandler[T](mapper), params: _*)
	}
	def dump(sql: String, params: Any*): Int = {
		query(sql, DBRunner.dumpHandler, params: _*)
	}

	/* ------------------------- impl ------------------------- */

	def insertReturnKey(sql: String, params: Any*): Long = {
		var conn: Connection = null
		var stmt: PreparedStatement = null
		var rs: ResultSet = null
		try {
			conn = prepareConnection
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
			fillAnyStatement(stmt, params: _*)
			stmt.executeUpdate
			rs = stmt.getGeneratedKeys
			if (rs.next) rs.getLong(1) else 0
		} catch {
			case e: SQLException => throw newException(e, sql, params: _*)
		} finally {
			DbUtils.closeQuietly(rs)
			DbUtils.closeQuietly(stmt)
			DbUtils.closeQuietly(conn)
		}
	}

	def query[T](sql: String, rsh: ResultSetHandler[T], params: Any*): T = {
		var conn: Connection = null
		var stmt: PreparedStatement = null
		var rs: ResultSet = null
		try {
			conn = prepareConnection

			// bell(2014-6): 避免大量数据时出现oom
			stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
			stmt.setFetchSize(8192)
			stmt.setFetchDirection(ResultSet.FETCH_REVERSE)

			fillAnyStatement(stmt, params: _*)
			rs = stmt.executeQuery
			rsh.handle(rs)
		} catch {
			case e: SQLException => throw newException(e, sql, params: _*)
		} finally {
			DbUtils.closeQuietly(rs)
			DbUtils.closeQuietly(stmt)
			DbUtils.closeQuietly(conn)
		}
	}

	def update(sql: String, params: Any*): Int = {
		var conn: Connection = null
		var stmt: PreparedStatement = null
		try {
			conn = prepareConnection
			stmt = conn.prepareStatement(sql)
			fillAnyStatement(stmt, params: _*)
			stmt.executeUpdate()
		} catch {
			case e: SQLException => throw newException(e, sql, params: _*)
		} finally {
			DbUtils.closeQuietly(stmt)
			DbUtils.closeQuietly(conn)
		}
	}

	private def fillAnyStatement(stmt: PreparedStatement, params: Any*) {
		for (i <- 0 until params.length; param = params(i)) {
			stmt.setObject(i + 1, param)
		}
	}
	private def newException(e: SQLException, sql: String, params: Any*): SQLException = {
		val msg = s"${e.getMessage} Query: $sql Parameters: ${params.mkString(", ")}"
		new SQLException(msg, e.getSQLState, e.getErrorCode)
	}

}

