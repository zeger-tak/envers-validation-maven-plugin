package org.tak.zeger.enversvalidationplugin.exceptions;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.entities.AuditTableInformation;

public class SetupValidationForSpecificWhitelistEntryException extends ValidationException
{
	private final AuditTableInformation auditTableInformation;

	public SetupValidationForSpecificWhitelistEntryException(@Nonnull String message, @Nonnull AuditTableInformation auditTableInformation)
	{
		super(message);
		this.auditTableInformation = auditTableInformation;
	}

	@Nonnull
	public AuditTableInformation getAuditTableInformation()
	{
		return auditTableInformation;
	}
}
