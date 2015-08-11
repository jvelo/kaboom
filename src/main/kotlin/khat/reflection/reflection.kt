package khat.reflection

import khat.table
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

public fun <A: Annotation, T> Class<T>.findAnnotationHierarchy(klass: Class<A>): A? {
    // FIXME
    // Find out why it gives java.lang.NoSuchFieldError: $kotlinClass when taking the klass parameter
    // as a KClass
    return this.getAnnotationsByType(klass).firstOrNull() ?:
        this.getSuperclass()?.findAnnotationHierarchy(klass)
}

