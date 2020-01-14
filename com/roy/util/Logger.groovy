#!/usr/bin/env groovy

package com.roy.util

/**
 * Distributed under GNU-GPL3 license.
 *
 * @author Anirban Roy (https://github.com/anirban-roy/)
 */
class Logger {

        def theClass

        /**
         * Formats the message with date and prints the message at INFO level to the stdout.
         */
        def log(level="INFO", msg) {
            def dateFormat = "MMM dd yyyy HH:mm:ss"
            def dateFormatted = new Date().format(dateFormat)
            println "${dateFormatted} ${level}  (${shorten(theClass.name)}) - ${msg}"
        }

        def error(msg) {
            log("ERROR", msg)
        }

        def warn(msg) {
            log("WARN", msg)
        }

        def info(msg) {
            log("INFO", msg)
        }

        def debug(msg) {
            log("DEBUG", msg)
        }

        def shorten(className) {
            def parts = className.split('\\.')
            if (parts.size() > 2) {
                return parts[0..-2].collect {it[0]}.join('.') + "." + parts[-1]
            }
            return className
        }

        /**
         * Logs an error message and prints a stack trace when the given code block throws an exception.
         * Useful to surround scripts to make sure any otherwise uncaught exception is logged.
         */
        def logIfThrows(closure) {
            logIfThrows '', closure
        }

        /**
         *  Prints an error with stack trace and any provided custom message
         */
        def logIfThrows(msg, closure) {
            try {
                closure()
            } catch (Throwable t) {
                def stackTraceWriter = new StringWriter()
                t.printStackTrace(new PrintWriter(stackTraceWriter))
                def stackTrace = stackTraceWriter.toString()
                error msg + ' ' + stacktrace
            }
        }
}