package org.tak.zeger.enversvalidationplugin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// TODO: Replace this with an annotation that makes it possible to tell the executor how to fill the related field.
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ListOfAuditTablesInDatabase
{
}
