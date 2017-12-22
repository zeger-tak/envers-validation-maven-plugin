package com.github.zeger_tak.enversvalidationplugin.exceptions;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;

public class SetupValidationForSpecificAuditTableInformationException extends ValidationException
{
	private final AuditTableInformation auditTableInformation;

	public SetupValidationForSpecificAuditTableInformationException(@Nonnull String message, @Nonnull AuditTableInformation auditTableInformation)
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
