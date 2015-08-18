package kaboom.reflection

public fun <A: Annotation, T> Class<T>.findAnnotationInHierarchy(klass: Class<A>): A? {
    // FIXME
    // Find out why it gives java.lang.NoSuchFieldError: $kotlinClass when taking the klass parameter
    // as a KClass
    return this.getAnnotationsByType(klass).firstOrNull() ?:
        this.getSuperclass()?.findAnnotationInHierarchy(klass)
}

