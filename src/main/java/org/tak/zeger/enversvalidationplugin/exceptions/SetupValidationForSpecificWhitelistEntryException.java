package org.tak.zeger.enversvalidationplugin.exceptions;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;

public class SetupValidationForSpecificWhitelistEntryException extends ValidationException
{
	private final WhitelistEntry whitelistEntry;

	public SetupValidationForSpecificWhitelistEntryException(@Nonnull String message, @Nonnull WhitelistEntry whitelistEntry)
	{
		super(message);
		this.whitelistEntry = whitelistEntry;
	}

	@Nonnull
	public WhitelistEntry getWhitelistEntry()
	{
		return whitelistEntry;
	}
}
