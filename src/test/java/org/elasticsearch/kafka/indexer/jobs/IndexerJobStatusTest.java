package org.elasticsearch.kafka.indexer.jobs;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexerJobStatusTest {
	@Test
	public void toStringTest() {
		IndexerJobStatus indexerJobStatus = new IndexerJobStatus(1, IndexerJobStatusEnum.Started, 2);
		String indJobStatus = "[IndexerJobStatus: {partition=2lastCommittedOffset=1jobStatus=Started}]";
		assertTrue(indexerJobStatus.toString().equalsIgnoreCase(indJobStatus));

	}
}
