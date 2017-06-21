package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.RevisionConstants;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@RunWith(Parameterized.class)
public class RevisionHistoryValidatorParameterizedTest
{
	private static final String REV_COLUMN = "rev";

	private final RevisionHistoryValidator validator;
	private final String expectedExceptionMessage;
	private final ConnectionProviderInstance connectionProvider;

	public RevisionHistoryValidatorParameterizedTest(@Nonnull String testName, @Nonnull Map<String, List<TableRow>> recordsInAuditTable, @Nullable String expectedExceptionMessage)
	{
		connectionProvider = mock(ConnectionProviderInstance.class);
		validator = new RevisionHistoryValidator(connectionProvider, "auditTableName", recordsInAuditTable);
		this.expectedExceptionMessage = expectedExceptionMessage;
	}

	@Before
	public void init()
	{
		final DatabaseQueries databaseQueries = mock(DatabaseQueries.class);
		when(connectionProvider.getQueries()).thenReturn(databaseQueries);
		when(databaseQueries.getRevTypeColumnName()).thenReturn(REV_COLUMN);
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data()
	{
		final String id1 = "primaryId1";
		final String id2 = "primaryId2";
		final String expectedExceptionMessageId1 = "The following identifiers [" + id1 + "] have an invalid audit history for the table auditTableName";
		final String expectedExceptionMessageId2 = "The following identifiers [" + id2 + "] have an invalid audit history for the table auditTableName";
		final String expectedExceptionMessageId1And2 = "The following identifiers [" + id1 + ", " + id2 + "] have an invalid audit history for the table auditTableName";

		final TableRow addRevision = createAddRevision();
		final TableRow modifyRevision = createModifyRevision();
		final TableRow removeRevision = createRemoveRevision();
		final TableRow doNotValidateRevision = createDoNotValidateRevision();

		final List<TableRow> validHistory = Collections.singletonList(addRevision);
		final List<TableRow> invalidHistory = Collections.singletonList(modifyRevision);

		final Map<String, List<TableRow>> validHistory1ValidHistory2 = new HashMap<>();
		validHistory1ValidHistory2.put(id1, validHistory);
		validHistory1ValidHistory2.put(id2, validHistory);

		final Map<String, List<TableRow>> validHistory1InvalidHistory2 = new HashMap<>();
		validHistory1InvalidHistory2.put(id1, validHistory);
		validHistory1InvalidHistory2.put(id2, invalidHistory);

		final Map<String, List<TableRow>> invalidHistory1ValidHistory2 = new HashMap<>();
		invalidHistory1ValidHistory2.put(id1, invalidHistory);
		invalidHistory1ValidHistory2.put(id2, validHistory);

		final Map<String, List<TableRow>> invalidHistory1InvalidHistory2 = new HashMap<>();
		invalidHistory1InvalidHistory2.put(id1, invalidHistory);
		invalidHistory1InvalidHistory2.put(id2, invalidHistory);

		return Arrays.asList(new Object[][] {
				//@formatter:off
				// No history records
				{"noHistoryAtAll", Collections.emptyMap(), null},
				{"noHistoryButFoundTableName", 		Collections.singletonMap(id1,		Collections.emptyList()),	 null},
				
				// Single rows
				{"onlyAdd",				Collections.singletonMap(id1, Collections.singletonList(addRevision)), 				null},
				{"onlyModify",			Collections.singletonMap(id1, Collections.singletonList(modifyRevision)), 			expectedExceptionMessageId1},
				{"onlyRemove",			Collections.singletonMap(id1, Collections.singletonList(removeRevision)), 			expectedExceptionMessageId1},
				{"onlyDoNotValidate",	Collections.singletonMap(id1, Collections.singletonList(doNotValidateRevision)),	null},
				// "Do not validate" revisions will not be included in the testcases below, as databaseQueries.getRevTypeColumnName is expected to be nonnull
				
				// Two rows for one identifier
				{"firstAddThenAnotherAdd",			Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision)),			expectedExceptionMessageId1},
				{"firstAddThenModify",				Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision)),		null},
				{"firstAddThenRemove",				Collections.singletonMap(id1, Arrays.asList(addRevision, removeRevision)),		null},
				{"firstModifyThenAdd",				Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstModifyThenAnotherModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, modifyRevision)),	expectedExceptionMessageId1},
				{"firstModifyThenRemove",			Collections.singletonMap(id1, Arrays.asList(modifyRevision, removeRevision)),	expectedExceptionMessageId1},
				{"firstRemoveThenAdd",				Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstRemoveThenModify",			Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstRemoveThenRemove",			Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		expectedExceptionMessageId1},
				
				// Three rows for one identifier
				{"firstAddThenAddThenAdd",			Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstAddThenAddThenModify",		Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, modifyRevision)),		expectedExceptionMessageId1},
				{"firstAddThenAddThenRemove",		Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, removeRevision)),		expectedExceptionMessageId1},
				{"firstAddThenModifyThenAdd",		Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstAddThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, modifyRevision)),	null},
				{"firstAddThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, removeRevision)),	null},
				
				{"firstModifyThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstModifyThenAddThenModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, modifyRevision)),	expectedExceptionMessageId1},
				{"firstModifyThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, removeRevision)),	expectedExceptionMessageId1},
				{"firstModifyThenModifyThenAdd",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstModifyThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, modifyRevision)),	expectedExceptionMessageId1},
				{"firstModifyThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, removeRevision)),	expectedExceptionMessageId1},
				
				{"firstRemoveThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, addRevision)),			expectedExceptionMessageId1},
				{"firstRemoveThenAddThenModify",	Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, modifyRevision)),		expectedExceptionMessageId1},
				{"firstRemoveThenAddThenRemove",	Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, removeRevision)),		expectedExceptionMessageId1},
				{"firstRemoveThenModifyThenAdd",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, addRevision)),		expectedExceptionMessageId1},
				{"firstRemoveThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, modifyRevision)),	expectedExceptionMessageId1},
				{"firstRemoveThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, removeRevision)),	expectedExceptionMessageId1},
				
				// One rows for two identifiers
				{"firstIdentifierValidHistorySecondIdentifierValidHistory",		validHistory1ValidHistory2,		null},
				{"firstIdentifierValidHistorySecondIdentifierInvalidHistory",	validHistory1InvalidHistory2,	expectedExceptionMessageId2},
				{"firstIdentifierInvalidHistorySecondIdentifierInvalidHistory",	invalidHistory1ValidHistory2,	expectedExceptionMessageId1},
				{"firstIdentifierInvalidHistorySecondIdentifierInvalidHistory",	invalidHistory1InvalidHistory2,	expectedExceptionMessageId1And2},
			//@formatter:on
		});
	}

	@Nonnull
	private static TableRow createAddRevision()
	{
		final TableRow revision = new TableRow();
		revision.addColumn(REV_COLUMN, BigDecimal.valueOf(RevisionConstants.ADD_REVISION));
		return revision;
	}

	@Nonnull
	private static TableRow createModifyRevision()
	{
		final TableRow revision = new TableRow();
		revision.addColumn(REV_COLUMN, BigDecimal.valueOf(RevisionConstants.UPDATE_REVISION));
		return revision;
	}

	@Nonnull
	private static TableRow createRemoveRevision()
	{
		final TableRow revision = new TableRow();
		revision.addColumn(REV_COLUMN, BigDecimal.valueOf(RevisionConstants.REMOVE_REVISION));
		return revision;
	}

	@Nonnull
	private static TableRow createDoNotValidateRevision()
	{
		final TableRow revision = new TableRow();
		revision.addColumn(REV_COLUMN, RevisionConstants.DO_NOT_VALIDATE_REVISION);
		return revision;
	}

	@Test
	public void test()
	{
		if (expectedExceptionMessage == null)
		{
			validator.validateHistoryIsAValidFlow();
		}
		else
		{
			try
			{
				validator.validateHistoryIsAValidFlow();
				fail("Expected a " + ValidationException.class.getSimpleName() + " with the following message " + expectedExceptionMessage);
			}
			catch (ValidationException e)
			{
				assertEquals("Caught a " + ValidationException.class.getSimpleName() + " as expected, but exception message was different: ", expectedExceptionMessage, e.getMessage());
			}
		}
	}
}