package org.elasticsearch.kafka.indexer.service.jmx;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatus;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatusEnum;
import org.elasticsearch.kafka.indexer.service.JobManagerService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaEsIndexerStatusTest {
List<IndexerJobStatus> statuses = new ArrayList<IndexerJobStatus>();
	
	KafkaEsIndexerStatus kafkaEsIndexerStatus;
	
	@Mock
	private JobManagerService jobManager;
	
	@Before
	public void init(){
		statuses.add(new IndexerJobStatus(123, IndexerJobStatusEnum.Failed, 1));
		statuses.add(new IndexerJobStatus(124, IndexerJobStatusEnum.Cancelled, 2));
		statuses.add(new IndexerJobStatus(125, IndexerJobStatusEnum.Stopped, 3));
		statuses.add(new IndexerJobStatus(126, IndexerJobStatusEnum.Started, 4));
		statuses.add(new IndexerJobStatus(127, IndexerJobStatusEnum.Failed, 5));
		statuses.add(new IndexerJobStatus(130, IndexerJobStatusEnum.Hanging, 8));

		when(jobManager.getJobStatuses()).thenReturn(statuses);
		kafkaEsIndexerStatus = new KafkaEsIndexerStatus(jobManager);
	}
	
	@Test
	public void getStatuses(){
		assertEquals(kafkaEsIndexerStatus.getStatuses().equals(statuses), true);
	}
	
	@Test
	public void getCountOfFailedJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfFailedJobs(), 2);
	}
	
	@Test
	public void getCountOfCancelledJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfCancelledJobs(), 1);
	}
	
	@Test
	public void getCountOfStoppedJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfStoppedJobs(), 1);
	}
	
	@Test
	public void getCountOfHangingJobs(){
		assertEquals(kafkaEsIndexerStatus.getCountOfHangingJobs(), 1);
	}
	
	@Test
	public void isAlive(){
		assertEquals(kafkaEsIndexerStatus.isAlive(), true);
	}
}
