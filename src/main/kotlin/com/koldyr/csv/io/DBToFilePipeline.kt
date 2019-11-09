package com.koldyr.csv.io

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang.StringUtils
import org.postgresql.core.Oid
import java.io.BufferedWriter
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.charset.StandardCharsets.*
import java.nio.file.Files
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern

/**
 * Pipeline to load data from db table into csv file. Blob/clob columns will be stored in dedicated sub-folder
 *
 * @created: 2018.03.05
 */
class DBToFilePipeline @Throws(IOException::class)
constructor(fileName: String) : BaseDBPipeline(), Closeable {

    private val output: BufferedWriter

    private var blobDir: File? = null
    private val csvFile: File = File(fileName)

    init {
        val dir = csvFile.parentFile
        if (!dir.exists()) {
            dir.mkdirs()
        }
        output = Files.newBufferedWriter(csvFile.toPath(), UTF_8)
    }

    @Throws(IOException::class)
    fun flush() {
        output.flush()
    }

    @Throws(IOException::class)
    override fun close() {
        output.flush()
        output.close()
    }

    @Throws(SQLException::class, IOException::class)
    fun next(resultSet: ResultSet, columnCount: Int): Boolean {
        if (resultSet.next()) {
            output.write(composeRowData(resultSet, columnCount))
            return true
        }

        return false
    }

    @Throws(SQLException::class)
    private fun composeRowData(resultSet: ResultSet, columnCount: Int): String {
        val rowData = StringJoiner(",", "", "\n")
        for (columnIndex in 1..columnCount) {
            val value = getValue(resultSet, columnIndex)
            rowData.add(if (value == null) StringUtils.EMPTY else StringEscapeUtils.escapeCsv(value.toString()))
        }

        return rowData.toString()
    }

    @Throws(SQLException::class)
    private fun getValue(resultSet: ResultSet, columnIndex: Int): Any? {
        val columnType = getColumnType(resultSet.metaData, columnIndex)

        when (columnType) {
            Types.VARCHAR, Types.NVARCHAR, Types.NCHAR, Types.CHAR -> {
                var value: String? = resultSet.getString(columnIndex) ?: return null

                val carriageReturn = CARRIAGE_RETURN.matcher(value!!)
                if (carriageReturn.find()) {
                    value = carriageReturn.replaceAll(CR_REPLACEMENT)
                }
                return value!!.trim { it <= ' ' }
            }
            Types.INTEGER -> return resultSet.getInt(columnIndex)
            Types.FLOAT -> return resultSet.getFloat(columnIndex)
            Types.DATE -> {
                val date = resultSet.getDate(columnIndex) ?: return null

                return DateTimeFormatter.ISO_LOCAL_DATE.format(date.toLocalDate())
            }
            Types.TIMESTAMP -> {
                val timestamp = resultSet.getTimestamp(columnIndex) ?: return null

                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(timestamp.toLocalDateTime())
            }
            Types.NUMERIC -> return resultSet.getBigDecimal(columnIndex)
            Types.CLOB, Types.NCLOB -> return saveCharStream(resultSet, columnIndex)
            Types.BLOB, Types.BINARY, Oid.TEXT -> return saveBinaryStream(resultSet, columnIndex)
            else -> return StringUtils.EMPTY
        }
    }

    @Throws(SQLException::class)
    private fun saveBinaryStream(resultSet: ResultSet, columnIndex: Int): String {
        if (blobDir == null) {
            createBlobSubFolder()
        }

        val blobId = UUID.randomUUID().toString()
        val blobFile = File(blobDir, blobId + BLOB_FILE_EXT)
        try {
            resultSet.getBinaryStream(columnIndex).use { inputStream ->
                FileOutputStream(blobFile).use { outputStream ->
                    IOUtils.copy(inputStream, outputStream)
                    outputStream.flush()
                }
            }
        } catch (e: IOException) {
            throw SQLException(e)
        }

        return blobId
    }

    @Throws(SQLException::class)
    private fun saveCharStream(resultSet: ResultSet, columnIndex: Int): String {
        if (blobDir == null) {
            createBlobSubFolder()
        }

        val blobId = UUID.randomUUID().toString()
        val blobFile = File(blobDir, blobId + BLOB_FILE_EXT)
        try {
            resultSet.getCharacterStream(columnIndex).use { reader ->
                FileOutputStream(blobFile).use { outputStream ->
                    IOUtils.copy(reader, outputStream, UTF_8)
                    outputStream.flush()
                }
            }
        } catch (e: IOException) {
            throw SQLException(e)
        }

        return blobId
    }

    private fun createBlobSubFolder() {
        blobDir = File(csvFile.parentFile, csvFile.nameWithoutExtension)
        if (!blobDir!!.exists()) {
            blobDir!!.mkdirs()
        }
    }

    companion object {
        const val BLOB_FILE_EXT = ".bin"
        private val CARRIAGE_RETURN = Pattern.compile("[\n\r]+")
        private const val CR_REPLACEMENT = "\\\\n"
    }
}
