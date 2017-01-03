package com.goibibo.sqlshift.commons

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Timer.Context
import com.codahale.metrics._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/2/17.
  */
object MetricsWrapper {

    val metricRegistry: MetricRegistry = new MetricRegistry()

    private val logger: Logger = LoggerFactory.getLogger(MetricsWrapper.getClass)

    private val slf4jReporter: Slf4jReporter = Slf4jReporter.forRegistry(metricRegistry)
            .outputTo(LoggerFactory.getLogger("SQLShiftMetrics"))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build()
    private val jmxReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).build()

    /**
      * Starting log reporting using slf4j.
      *
      * @param period time to print metrics in logs
      */
    def startSLF4JReporting(period: Long): Unit = {
        logger.info("Starting slf4j reporter with period: {}", period)
        slf4jReporter.start(period, TimeUnit.SECONDS)
    }

    /**
      * Start JMX reporting.
      */
    def startJMXReporting(): Unit = {
        jmxReporter.start()
    }

    /**
      * Stop slf4j reporting
      */
    def stopSLF4JReporting(): Unit = {
        slf4jReporter.stop()
    }

    def stopJMXReporting(): Unit = {
        jmxReporter.stop()
    }

    def getTimerMetrics(metricName: String): Context = {
        val timer: Timer = metricRegistry.timer(metricName)
        timer.time()
    }

    def stopTimerContext(context: Context): Long = {
        context.stop()
    }

    def registerGauge(metricName: String, value: Boolean): Gauge[Boolean] = {
        try {
            metricRegistry.register(metricName, new Gauge[Boolean] {
                override def getValue: Boolean = {
                    value
                }
            })
        } catch {
            case e: IllegalArgumentException => logger.warn(s"$metricName gauge metric is already registered!!!")
                metricRegistry.getGauges.get(metricName).asInstanceOf[Gauge[Boolean]]
        }
    }

    def registerGauge(metricName: String, value: Int): Gauge[Int] = {
        try {
            metricRegistry.register(metricName, new Gauge[Int] {
                override def getValue: Int = {
                    value
                }
            })
        } catch {
            case e: IllegalArgumentException => logger.warn(s"$metricName gauge metric is already registered!!!")
                metricRegistry.getGauges.get(metricName).asInstanceOf[Gauge[Int]]
        }
    }

    def incCounter(metricName: String, incValue: Long = 1): Unit = {
        metricRegistry.counter(metricName).inc(incValue)
    }

}
