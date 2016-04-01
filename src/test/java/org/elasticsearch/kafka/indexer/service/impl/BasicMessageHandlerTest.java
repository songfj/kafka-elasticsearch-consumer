/**
  * @author marinapopova
  * Mar 29, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl;

import static org.junit.Assert.*;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IIndexHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.*;

/**
 * @author marinapopova
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicMessageHandlerTest {

	@Mock
    private ElasticSearchClientService elasticSearchClientService;
	@Mock
    private IIndexHandler elasticIndexHandler;

	@InjectMocks
	private BasicMessageHandler basicMessageHandler = new BasicMessageHandler();
	
	private BulkRequestBuilder mockedBulkRequestBuilder = Mockito.mock(BulkRequestBuilder.class);
	private IndexRequestBuilder mockedIndexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
	private ListenableActionFuture<BulkResponse> mockedActionFuture = Mockito.mock(ListenableActionFuture.class);
	private BulkResponse mockedBulkResponse = Mockito.mock(BulkResponse.class);
	private String testIndexName = "unitTestsIndex";
	private String testIndexType = "unitTestsType";
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// Mock all required ES classes and methods
	       Mockito.when(elasticSearchClientService.prepareBulk()).thenReturn(mockedBulkRequestBuilder);
	       Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString()))
	       		.thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(elasticSearchClientService.prepareIndex(Matchers.anyString(), Matchers.anyString(), Matchers.anyString()))
      			.thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(mockedIndexRequestBuilder.setSource(Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
	       Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
	       Mockito.when(mockedActionFuture.actionGet()).thenReturn(mockedBulkResponse);
	       // mock the number of messages in the bulk index request to be 1
	       Mockito.when(mockedBulkRequestBuilder.numberOfActions()).thenReturn(1);
	       
	       Mockito.when(elasticIndexHandler.getIndexName(null)).thenReturn(testIndexName);
	       Mockito.when(elasticIndexHandler.getIndexType(null)).thenReturn(testIndexType);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#prepareForPostToElasticSearch(java.util.Iterator)}.
	 */
	//@Test
	public void testPrepareForPostToElasticSearch() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#processMessage(byte[])}.
	 */
	@Test
	public void testProcessMessage() {
		String message = "test message";
		try {
			basicMessageHandler.processMessage(message.getBytes());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexName(null);
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexType(null);
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message);
		// be default, no routing should be set
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(0)).setRouting(Mockito.anyString());
	}

	@Test
	public void testProcessMessageAndPostToEs() {
	    // test happy path - no failures from indexing
	    Mockito.when(mockedBulkResponse.hasFailures()).thenReturn(false);
		String message = "test message";
		try {
			basicMessageHandler.processMessage(message.getBytes());
			basicMessageHandler.postToElasticSearch();
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexName(null);
		Mockito.verify(elasticIndexHandler, Mockito.times(1)).getIndexType(null);
		Mockito.verify(elasticSearchClientService, Mockito.times(1)).prepareIndex(testIndexName, testIndexType);
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(1)).setSource(message);
		// be default, no routing should be set
		Mockito.verify(mockedIndexRequestBuilder, Mockito.times(0)).setRouting(Mockito.anyString());
		
		Mockito.verify(mockedBulkRequestBuilder, Mockito.times(1)).execute();
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#addEventToBulkRequest(java.lang.String, java.lang.String, java.lang.String, boolean, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testAddEventToBulkRequest() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#getBulkRequestBuilder(java.lang.String)}.
	 */
	//@Test
	public void testGetBulkRequestBuilder() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler#postToElasticSearch()}.
	 */
	@Test
	public void testPostToElasticSearch() {
		fail("Not yet implemented");
	}

}
