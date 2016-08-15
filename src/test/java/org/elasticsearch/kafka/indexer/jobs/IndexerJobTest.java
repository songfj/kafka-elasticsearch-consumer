package org.elasticsearch.kafka.indexer.jobs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientRecoverableException;
import org.elasticsearch.kafka.indexer.jobs.IndexerJob.BatchCreationResult;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.KafkaClientService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

@RunWith(MockitoJUnitRunner.class)
public class IndexerJobTest {

	@Mock
	private IMessageHandler messageHandler;

	@Mock
	private KafkaClientService kafkaClientService;

	private IndexerJob indexerJob;

	private FetchResponse fetchResponse = Mockito.mock(FetchResponse.class);
	private ByteBufferMessageSet byteBufferMsgSet = Mockito.mock(ByteBufferMessageSet.class);
	List<Message> messages = new ArrayList<Message>();
	IndexerJob indexerJobSpy;

	@Before
	public void setUp() throws Exception {

		String testMessage = "testMessage";
		String testMessage2 = "testMessage2";
		Message message = new Message(testMessage.getBytes());
		Message message2 = new Message(testMessage2.getBytes());

		messages.add(message);
		messages.add(message2);
		byteBufferMsgSet = new ByteBufferMessageSet(messages);
		Mockito.when(fetchResponse.messageSet(Matchers.anyString(), Matchers.anyInt())).thenReturn(byteBufferMsgSet);
		String topic = "testTopic";
		int partition = 0;
		int consumerSleepBetweenFetchsMs = 100;

		indexerJob = new IndexerJob(topic, messageHandler, kafkaClientService, partition, consumerSleepBetweenFetchsMs);
		indexerJob.setPerfReportingEnabled(true);
		indexerJobSpy = Mockito.spy(indexerJob);
	}
	
	@Test
	public void processMessagesFromKafka() {
		try {
			Mockito.when(kafkaClientService.getMessagesFromKafka(Matchers.anyLong())).thenReturn(fetchResponse);

			indexerJob.processMessagesFromKafka();
			Mockito.verify(messageHandler, Mockito.times(messages.size())).addMessageToBatch(Matchers.any(), Matchers.anyLong());

		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
	}

	@Test(expected = KafkaClientRecoverableException.class)
	public void processMessagesFromKafka_Exception() throws KafkaClientRecoverableException {

		Mockito.when(kafkaClientService.getMessagesFromKafka(Matchers.anyLong())).thenReturn(fetchResponse);
		Mockito.doThrow(new KafkaClientRecoverableException()).when(kafkaClientService)
				.saveOffsetInKafka(Matchers.anyLong(), Matchers.anyShort());
		try {
			indexerJob.processMessagesFromKafka();
		
		} catch (KafkaClientRecoverableException e) {
			throw e;
		} catch (Exception e1) {
			fail("Unexpected exception from unit test: " + e1.getMessage());
		}

	}

	@Test
	public void postBatchToElasticSearch() {
		try {
			Mockito.when(messageHandler.postToElasticSearch()).thenReturn(true);
			indexerJob.postBatchToElasticSearch(0L);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		try {
			Mockito.when(messageHandler.postToElasticSearch()).thenThrow(new IndexerESRecoverableException());
			assertTrue(indexerJob.postBatchToElasticSearch(0L) == false);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

	}

	@Test
	public void postBatchToElasticSearch_ESException() {

		try {
			Mockito.when(messageHandler.postToElasticSearch()).thenThrow(new IndexerESRecoverableException("Exception"));
			//verify that postBatchToElasticSearch will not throw exception
			indexerJob.postBatchToElasticSearch(0L);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

	}

	@Test
	public void getMessagesAndOffsets() {
		try {
			Mockito.when(kafkaClientService.getMessagesFromKafka(0l)).thenReturn(fetchResponse);
			Mockito.when(kafkaClientService.handleErrorFromFetchMessages(Matchers.anyShort(), Matchers.anyLong()))
					.thenReturn(1L);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		Mockito.when(fetchResponse.hasError()).thenReturn(true);
		try {
			assertTrue(indexerJob.getMessageAndOffsets(0l) == null);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

	}

	@Test
	public void getMessagesAndOffsets_NoData() {
		ByteBufferMessageSet byteBufferMsgSetMock = Mockito.mock(ByteBufferMessageSet.class);
		try {
			Mockito.when(kafkaClientService.getMessagesFromKafka(0l)).thenReturn(fetchResponse);
		} catch (KafkaClientRecoverableException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		Mockito.when(fetchResponse.hasError()).thenReturn(false);
		Mockito.when(fetchResponse.messageSet(Matchers.anyString(), Matchers.anyInt()))
				.thenReturn(byteBufferMsgSetMock);
		Mockito.when(byteBufferMsgSetMock.validBytes()).thenReturn(0);
		try {
			Mockito.when(kafkaClientService.getLastestOffset()).thenReturn(999L);
		} catch (KafkaClientRecoverableException e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		//Check if call to this methot will not throw any exception
		try {
			indexerJob.getMessageAndOffsets(0l);
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

	}

	@Test
	public void addMessagesToBatchTest() {
		// verify that 2 messages were processed
		BatchCreationResult batchCreationResult = indexerJob.addMessagesToBatch(0, byteBufferMsgSet);
		long offset = batchCreationResult.getOffsetOfNextBatch();
		assertTrue(offset == messages.size());
		try {
			Mockito.verify(messageHandler, Mockito.times(messages.size())).addMessageToBatch(Matchers.any(),
					Matchers.anyLong());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
	}

	@Test
	public void addMessagesToBatchTest_Exception() {
		// verify that 2 messages were processed
		try {
			Mockito.when(messageHandler.transformMessage(Matchers.any(), Matchers.anyLong()))
					.thenThrow(new Exception());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		indexerJob.addMessagesToBatch(0, byteBufferMsgSet);
		try {
			Mockito.verify(messageHandler, Mockito.times(0)).addMessageToBatch(Matchers.any(), Matchers.anyLong());
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}

	}

	@Test
	public void determineOffsetForThisRound() {
		long nextOffset = 10000L;
		indexerJob.setNextOffsetToProcess(99999);
		try {
			Mockito.when(kafkaClientService.computeInitialOffset()).thenReturn(nextOffset);
			indexerJob.determineOffsetForThisRound();
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		assertTrue(indexerJob.getIndexerJobStatus().getLastCommittedOffset() == nextOffset);
		
		nextOffset = 99999;
		indexerJob.setNextOffsetToProcess(nextOffset);
		try {
			indexerJob.determineOffsetForThisRound();
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		assertTrue(indexerJob.getIndexerJobStatus().getLastCommittedOffset() == nextOffset);
	}
	
	

	@Test
	public void stopClients() {
		indexerJob.stopClients();
		Mockito.verify(kafkaClientService, Mockito.times(1)).close();
	}

	@Test
	public void reinitKafkaSucessfulTest_Exception() {
		try {
			Mockito.doThrow(new KafkaClientRecoverableException()).when(kafkaClientService).reInitKafka();
		} catch (Exception e) {
			fail("Unexpected exception from unit test: " + e.getMessage());
		}
		boolean result = indexerJob.reinitKafkaSucessful(null);
		assertTrue(indexerJob.getIndexerJobStatus().getJobStatus().equals(IndexerJobStatusEnum.Failed));
		assertFalse(result);
	}

	@Test
	public void reinitKafkaSucessfulTest() {
		boolean result = indexerJob.reinitKafkaSucessful(null);
		assertTrue(result);
	}

}
