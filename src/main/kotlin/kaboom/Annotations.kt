package kaboom

import kotlin.annotation.Retention
import kotlin.annotation.Target

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
public annotation class table(val name: String = "")

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FIELD)
public annotation class column(val name: String)

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
public annotation class filter(val where: String)
