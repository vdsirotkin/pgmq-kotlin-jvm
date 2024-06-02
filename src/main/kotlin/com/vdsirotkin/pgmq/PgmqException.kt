package com.vdsirotkin.pgmq

class PgmqException(message: String, cause: Throwable? = null) : Exception(message, cause)