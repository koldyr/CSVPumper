package com.koldyr.csv.io

import java.io.BufferedReader
import java.io.CharArrayWriter
import java.io.Closeable
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.math.BigDecimal
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.*
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.*
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.regex.Pattern
import kotlin.io.path.nameWithoutExtension
import org.apache.commons.lang3.StringUtils
import org.apache.commons.text.StringEscapeUtils.*
import org.postgresql.core.Oid
import com.koldyr.csv.io.DBToFilePipeline.Companion.BLOB_FILE_EXT

/**
 * Pipeline to load data from cvs file to database table
 *
 * @created: 2018.03.07
 */
class FileToDBPipeline @Throws(IOException::class)
constructor(csvFile: Path) : BaseDBPipeline(), Closeable {

    private val fileLock = ReentrantReadWriteLock()

    private val reader: BufferedReader

    private val counter = AtomicLong()

    private val blobDir: Path

    private val blobStreams = mutableListOf<InputStream>()

    init {
        blobDir = Paths.get(csvFile.parent.toString(), csvFile.nameWithoutExtension)
        createDirectories(blobDir)
        reader = newBufferedReader(csvFile, UTF_8)
    }

    fun counter(): Long {
        return counter.toLong()
    }

    @Throws(SQLException::class, IOException::class)
    fun next(statement: PreparedStatement, metaData: ResultSetMetaData): Boolean {
        val rowData = readLine() ?: return false

        try {
            setRowValues(statement, metaData, rowData)
            statement.addBatch()
        } catch (e: Exception) {
            throw SQLException(rowData, e)
        }

        return true
    }

    @Throws(IOException::class)
    private fun readLine(): String? {
        val lock = fileLock.readLock()
        lock.lock()
        try {
            counter.incrementAndGet()
            return reader.readLine()
        } finally {
            lock.unlock()
        }
    }

    @Throws(SQLException::class)
    private fun setRowValues(statement: PreparedStatement, metaData: ResultSetMetaData, rowData: String) {
        var escaped = false
        var columnIndex = 1
        val tokenBuffer = CharArrayWriter(256)
        val rowBuffer = CharBuffer.wrap(rowData)
        while (rowBuffer.hasRemaining()) {
            val c = rowBuffer.get()

            if (escaped) {
                if (c == '"') {
                    escaped = false
                }
            } else {
                escaped = c == '"'
            }

            if (c == ',' && !escaped) {
                setValue(metaData, statement, columnIndex, tokenBuffer.toString())

                tokenBuffer.reset()
                columnIndex++

                continue
            }

            tokenBuffer.append(c)
        }

        setValue(metaData, statement, columnIndex, tokenBuffer.toString())
    }

    @Throws(SQLException::class)
    private fun setValue(metaData: ResultSetMetaData, statement: PreparedStatement, columnIndex: Int, value: String?) {
        val columnType = getColumnType(metaData, columnIndex)

        if (isString(columnType)) {
            if (value == null) {
                statement.setNull(columnIndex, columnType)
                return
            }
        } else {
            if (StringUtils.isEmpty(value)) {
                statement.setNull(columnIndex, columnType)
                return
            }
        }

        var v = unescapeCsv(value)
        val carriageReturn = CARRIAGE_RETURN.matcher(v)
        if (carriageReturn.find()) {
            v = carriageReturn.replaceAll(CR_REPLACEMENT)
        }

        when (columnType) {
            Types.VARCHAR, Types.NVARCHAR, Types.NCHAR, Types.CHAR -> statement.setString(columnIndex, v)
            Types.INTEGER -> statement.setInt(columnIndex, Integer.parseInt(v))
            Types.BIGINT -> statement.setLong(columnIndex, java.lang.Long.parseLong(v))
            Types.FLOAT -> statement.setFloat(columnIndex, java.lang.Float.parseFloat(v))
            Types.DATE -> {
                val date = try {
                    LocalDate.parse(v, ISO_LOCAL_DATE)
                } catch (e: DateTimeParseException) {
                    LocalDate.parse(v, ISO_LOCAL_DATE_TIME)
                }

                statement.setDate(columnIndex, Date.valueOf(date))
            }
            Types.TIMESTAMP -> {
                val dateTime = LocalDateTime.parse(v, ISO_LOCAL_DATE_TIME)
                statement.setTimestamp(columnIndex, Timestamp.valueOf(dateTime))
            }
            Types.NUMERIC -> statement.setBigDecimal(columnIndex, BigDecimal(v))
            Types.BLOB, Types.CLOB, Types.NCLOB, Types.BINARY, Oid.TEXT -> setLOB(statement, columnIndex, value!!, columnType)
            else -> statement.setObject(columnIndex, v, columnType)
        }
    }

    @Throws(SQLException::class)
    private fun setLOB(statement: PreparedStatement, columnIndex: Int, blobId: String, columnType: Int) {
        try {
            val lob = newInputStream(Paths.get(blobDir.toString(), blobId + BLOB_FILE_EXT))
            blobStreams.add(lob)

            setLOB(statement, columnIndex, columnType, lob)
        } catch (e: FileNotFoundException) {
            throw SQLException(e)
        }
    }

    @Throws(IOException::class)
    override fun close() {
        reader.close()
    }

    fun closeBatch() {
        blobStreams.forEach { inputStream ->
            try {
                inputStream.close()
            } catch (e: IOException) {
                throw RuntimeException(e)
            }
        }
        blobStreams.clear()
    }

    companion object {
        private val CARRIAGE_RETURN = Pattern.compile("\\\\n")
        private const val CR_REPLACEMENT = "\n"
    }
}
