package com.github.zeger_tak.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.connection.DatabaseQueries;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.entities.RevisionConstants;
import com.github.zeger_tak.enversvalidationplugin.entities.TableRow;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RevisionValidatorHistoryFlowParameterizedTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static final String REV_COLUMN = "rev";

	private final RevisionValidator validator;
	private final ConnectionProviderInstance connectionProvider;
	private final String expectedExceptionMessageValidFlow;
	private final String expectedExceptionMessageAddOrModifyContent;

	public RevisionValidatorHistoryFlowParameterizedTest(@Nonnull String testName, @Nonnull Map<String, List<TableRow>> recordsInAuditTable, @Nonnull Map<String, TableRow> recordsInContentTable, @Nullable String expectedExceptionMessageValidFlow, @Nullable String expectedExceptionMessageAddOrModifyContent)
	{
		connectionProvider = mock(ConnectionProviderInstance.class);
		validator = new RevisionValidator(connectionProvider, new AuditTableInformation("auditTableName", "auditTableName"), recordsInAuditTable, recordsInContentTable);
		this.expectedExceptionMessageValidFlow = expectedExceptionMessageValidFlow;
		this.expectedExceptionMessageAddOrModifyContent = expectedExceptionMessageAddOrModifyContent;
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
		final String expectedExceptionMessageValidFlowId1 = "The following identifiers [" + id1 + "] have an invalid audit history in auditTableName for the table auditTableName";
		final String expectedExceptionMessageValidFlowId2 = "The following identifiers [" + id2 + "] have an invalid audit history in auditTableName for the table auditTableName";
		final String expectedExceptionMessageValidFlowId1And2 = "The following identifiers [" + id1 + ", " + id2 + "] have an invalid audit history in auditTableName for the table auditTableName";

		final String expectedExceptionMessageAddModifyId1 = "The following identifiers [" + id1 + "] have a latest revision of type Add/Modify but have no record present in content table auditTableName.";
		final String expectedExceptionMessageAddModifyId1And2 = "The following identifiers [" + id1 + ", " + id2 + "] have a latest revision of type Add/Modify but have no record present in content table auditTableName.";

		final String expectedExceptionMessageForTableWithoutRevtypeColumn = "The audit table auditTableName does not have a column referring to the revision table.";

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
				{"noHistoryAtAll",                  Collections.emptyMap(),                                                                      Collections.emptyMap(),   null, null},
				{"noHistoryButFoundTableName", 		Collections.singletonMap(id1, Collections.emptyList()),                                      Collections.emptyMap(),   null, null},

				// Single rows
				{"onlyAdd",				            Collections.singletonMap(id1, Collections.singletonList(addRevision)), 				          Collections.emptyMap(),  null, expectedExceptionMessageAddModifyId1},
				{"onlyModify",			            Collections.singletonMap(id1, Collections.singletonList(modifyRevision)), 			          Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"onlyRemove",			            Collections.singletonMap(id1, Collections.singletonList(removeRevision)), 			          Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},
				{"onlyDoNotValidate",	            Collections.singletonMap(id1, Collections.singletonList(doNotValidateRevision)),	          Collections.emptyMap(),  expectedExceptionMessageForTableWithoutRevtypeColumn, expectedExceptionMessageForTableWithoutRevtypeColumn},
				// "Do not validate" revisions will not be included in the testcases below, as databaseQueries.getRevTypeColumnName is expected to be nonnull

				// Two rows for one identifier
				{"firstAddThenAnotherAdd",			Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision)),			              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstAddThenModify",				Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision)),		              Collections.emptyMap(),  null, expectedExceptionMessageAddModifyId1},
				{"firstAddThenRemove",				Collections.singletonMap(id1, Arrays.asList(addRevision, removeRevision)),		              Collections.emptyMap(),  null, null},
				{"firstModifyThenAdd",				Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision)),		              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenAnotherModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, modifyRevision)),	              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenRemove",			Collections.singletonMap(id1, Arrays.asList(modifyRevision, removeRevision)),	              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},
				{"firstRemoveThenAdd",				Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenModify",			Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenRemove",			Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision)),		              Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},

				// Three rows for one identifier
				{"firstAddThenAddThenAdd",			Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, addRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstAddThenAddThenModify",		Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, modifyRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstAddThenAddThenRemove",		Collections.singletonMap(id1, Arrays.asList(addRevision, addRevision, removeRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},
				{"firstAddThenModifyThenAdd",		Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, addRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstAddThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, modifyRevision)),	  Collections.emptyMap(),  null, expectedExceptionMessageAddModifyId1},
				{"firstAddThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(addRevision, modifyRevision, removeRevision)),	  Collections.emptyMap(),  null, null},

				{"firstModifyThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, addRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenAddThenModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, modifyRevision)),	  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, removeRevision)),	  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},
				{"firstModifyThenModifyThenAdd",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, addRevision)),		  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, modifyRevision)),	  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstModifyThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(modifyRevision, addRevision, removeRevision)),	  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},

				{"firstRemoveThenAddThenAdd",		Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, addRevision)),       Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenAddThenModify",	Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, modifyRevision)),    Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenAddThenRemove",	Collections.singletonMap(id1, Arrays.asList(removeRevision, addRevision, removeRevision)),	  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},
				{"firstRemoveThenModifyThenAdd",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, addRevision)),    Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenModifyThenModify",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, modifyRevision)), Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1},
				{"firstRemoveThenModifyThenRemove",	Collections.singletonMap(id1, Arrays.asList(removeRevision, modifyRevision, removeRevision)), Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, null},

				// One rows for two identifiers
				{"firstIdentifierValidHistorySecondIdentifierValidHistory",		validHistory1ValidHistory2,		                                  Collections.emptyMap(),  null, expectedExceptionMessageAddModifyId1And2},
				{"firstIdentifierValidHistorySecondIdentifierInvalidHistory",	validHistory1InvalidHistory2,	                                  Collections.emptyMap(),  expectedExceptionMessageValidFlowId2, expectedExceptionMessageAddModifyId1And2},
				{"firstIdentifierInvalidHistorySecondIdentifierInvalidHistory",	invalidHistory1ValidHistory2,	                                  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1, expectedExceptionMessageAddModifyId1And2},
				{"firstIdentifierInvalidHistorySecondIdentifierInvalidHistory",	invalidHistory1InvalidHistory2,	                                  Collections.emptyMap(),  expectedExceptionMessageValidFlowId1And2, expectedExceptionMessageAddModifyId1And2},

				// Valid history with latest Add/Modify and matching content in content table.
				{"addAndMatchingContent",	        Collections.singletonMap(id1, Collections.singletonList(addRevision)),                        Collections.singletonMap(id1, doNotValidateRevision), null, null},
				{"modifyAndMatchingContent",	    Collections.singletonMap(id1, Collections.singletonList(modifyRevision)),                     Collections.singletonMap(id1, doNotValidateRevision), expectedExceptionMessageValidFlowId1, null},
				{"removeAndMatchingContent",	    Collections.singletonMap(id1, Collections.singletonList(modifyRevision)),                     Collections.singletonMap(id1, doNotValidateRevision), expectedExceptionMessageValidFlowId1, null},
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
		revision.addColumn(REV_COLUMN, BigDecimal.valueOf(RevisionConstants.MODIFY_REVISION));
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
	public void testValidateHistoryIsAValidFlow()
	{
		if (expectedExceptionMessageValidFlow == null)
		{
			validator.validateHistoryIsAValidFlow();
		}
		else
		{
			expectedException.expect(ValidationException.class);
			expectedException.expectMessage(expectedExceptionMessageValidFlow);

			validator.validateHistoryIsAValidFlow();
		}
	}

	@Test
	public void testValidateLatestAddOrModfyRevisionRefersToExistingContent()
	{
		if (expectedExceptionMessageAddOrModifyContent == null)
		{
			validator.validateLatestAddOrModifyRevisionRefersToExistingContent();
		}
		else
		{
			expectedException.expect(ValidationException.class);
			expectedException.expectMessage(expectedExceptionMessageAddOrModifyContent);

			validator.validateLatestAddOrModifyRevisionRefersToExistingContent();
		}
	}
}