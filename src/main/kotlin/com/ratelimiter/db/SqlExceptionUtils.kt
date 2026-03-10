package com.ratelimiter.db

import org.jetbrains.exposed.exceptions.ExposedSQLException
import java.sql.BatchUpdateException
import java.sql.SQLException

private const val ORACLE_UNIQUE_CONSTRAINT_ERROR_CODE = 1

/**
 * Returns true if this [ExposedSQLException] was caused by an Oracle unique constraint
 * violation (ORA-00001). The wrapped cause is expected to be a [SQLException] whose
 * vendor error code is 1.
 */
fun ExposedSQLException.isDuplicateKeyViolation(): Boolean {
    val sqlException = cause as? SQLException ?: return false
    return sqlException.errorCode == ORACLE_UNIQUE_CONSTRAINT_ERROR_CODE
}

/**
 * Returns true if this [BatchUpdateException] was caused by an Oracle unique constraint
 * violation (ORA-00001). [BatchUpdateException] extends [SQLException], so its own
 * errorCode is checked directly.
 */
fun BatchUpdateException.isDuplicateKeyViolation(): Boolean {
    return errorCode == ORACLE_UNIQUE_CONSTRAINT_ERROR_CODE
}
