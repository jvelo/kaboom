package khat

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

Retention(RetentionPolicy.RUNTIME)
Target(ElementType.TYPE)
annotation class table(val name: String = "")

Retention(RetentionPolicy.RUNTIME)
Target(ElementType.PARAMETER)
annotation class column(val name: String)

Retention(RetentionPolicy.RUNTIME)
Target(ElementType.TYPE)
annotation class filter(val where: String)
