package com.koldyr.csv.model

/**
 * Description of class PageBlockData
 *
 * @created: 2018.03.06
 */
data class PageBlockData(
        val index: Int,
        val start: Long,
        val length: Long
) {

    override fun toString(): String {
        val sb = StringBuffer("PageBlockData{")
        sb.append("index=").append(index)
        sb.append(", start=").append(start)
        sb.append(", length=").append(length)
        sb.append('}')
        return sb.toString()
    }
}
