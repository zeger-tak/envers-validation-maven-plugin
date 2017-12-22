package com.github.zeger_tak.enversvalidationplugin.entities;

import org.hibernate.envers.RevisionType;

public final class RevisionConstants
{
	public static final Integer DO_NOT_VALIDATE_REVISION = null;
	public static final int ADD_REVISION = RevisionType.ADD.getRepresentation();
	public static final int MODIFY_REVISION = RevisionType.MOD.getRepresentation();
	public static final int REMOVE_REVISION = RevisionType.DEL.getRepresentation();

	private RevisionConstants()
	{
	}
}