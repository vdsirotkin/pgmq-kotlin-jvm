package com.vdsirotkin.pgmq.util

import java.sql.ResultSet

internal class ResultSetIterator<T>(
    private val resultSet: ResultSet,
    private val extractor: (ResultSet) -> T
) : Iterator<T> {
    override fun hasNext(): Boolean {
        return resultSet.next()
    }

    override fun next(): T {
        return extractor(resultSet)
    }
}

internal fun <T> ResultSet.asIterable(extractor: (ResultSet) -> T) = Iterable { ResultSetIterator(this, extractor) }