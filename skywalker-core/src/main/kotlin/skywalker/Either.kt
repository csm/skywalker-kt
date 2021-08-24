package skywalker

import java.lang.Exception
import java.lang.IllegalStateException

sealed interface Result<V> {
    fun isSuccess(): Boolean
    fun isFailure(): Boolean
    fun value(): V
    fun exception(): Throwable
}

data class Ok<V>(val value: V): Result<V> {
    override fun isSuccess(): Boolean = true
    override fun isFailure(): Boolean = false
    override fun value(): V = value
    override fun exception(): Throwable {
        throw IllegalStateException("not a failure")
    }
}

data class Err<V>(val exception: Throwable): Result<V> {
    override fun isSuccess(): Boolean = false
    override fun isFailure(): Boolean = true
    override fun value(): V {
        throw IllegalStateException("not a success")
    }
    override fun exception(): Throwable = exception
}