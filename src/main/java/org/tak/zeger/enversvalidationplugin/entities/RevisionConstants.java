package org.tak.zeger.enversvalidationplugin.entities;

import org.hibernate.envers.RevisionType;

public final class RevisionConstants
{
	public static final Integer DO_NOT_VALIDATE_REVISION = null; // TODO: Figure out a way to validate tables without a revtype column
	public static final int ADD_REVISION = RevisionType.ADD.getRepresentation();
	public static final int UPDATE_REVISION = RevisionType.MOD.getRepresentation();
	public static final int REMOVE_REVISION = RevisionType.DEL.getRepresentation();
	public static final String REVTYPE_COLUMN_NAME = "REVTYPE";

	private RevisionConstants()
	{
	}
}