package com.koldyr.csv.processor

import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.ProcessorContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.sql.SQLException
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.concurrent.Callable

/**
 * Description of class BasePageProcessor
 *
 * @created: 2018.03.10
 */
abstract class BasePageProcessor protected constructor(protected val tableName: String, protected val context: ProcessorContext) : Callable<Int> {
    protected val format: DecimalFormat

    init {
        val decimalFormatSymbols = DecimalFormatSymbols()
        decimalFormatSymbols.groupingSeparator = ','
        format = DecimalFormat("###,###,###,###", decimalFormatSymbols)
    }

    override fun call(): Int? {
        var processedBlocks = 0

        var pageBlock = context.getNextPageBlock(tableName)
        while (pageBlock != null) {
            try {
                execute(pageBlock)

                processedBlocks++
            } catch (e: Exception) {
                LoggerFactory.getLogger(javaClass).error(e.message, e)
                return processedBlocks
            }

            pageBlock = context.getNextPageBlock(tableName)
        }

        return processedBlocks
    }

    @Throws(SQLException::class, IOException::class)
    protected abstract fun execute(pageBlock: PageBlockData)
}
