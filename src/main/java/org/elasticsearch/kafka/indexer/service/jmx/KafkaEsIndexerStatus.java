package org.elasticsearch.kafka.indexer.service.jmx;

import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatus;
import org.elasticsearch.kafka.indexer.jobs.IndexerJobStatusEnum;
import org.elasticsearch.kafka.indexer.service.JobManagerService;

import java.util.List;

public class KafkaEsIndexerStatus implements KafkaEsIndexerStatusMXBean {

	protected JobManagerService jobManagerService;
	private int failedJobs;
	private int cancelledJobs;
	private int stoppedJobs;
	private int hangingJobs;

	public KafkaEsIndexerStatus(JobManagerService jobmanagerService) {
		this.jobManagerService = jobmanagerService;
	}

	public boolean isAlive() {
			return true;
	}

	public List<IndexerJobStatus> getStatuses() {
		return jobManagerService.getJobStatuses();
	}

	public int getCountOfFailedJobs() {
		failedJobs = 0;
		for (IndexerJobStatus jobStatus : jobManagerService.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Failed)){
				failedJobs++;
			}
		}
		return failedJobs;
	}

	public int getCountOfStoppedJobs() {
		stoppedJobs = 0;
		for (IndexerJobStatus jobStatus : jobManagerService.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Stopped)){
				stoppedJobs++;
			}
		}
		return stoppedJobs;
	}
	
	public int getCountOfHangingJobs() {
		hangingJobs = 0;
		for (IndexerJobStatus jobStatus : jobManagerService.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Hanging)){
				hangingJobs++;
			}
		}
		return hangingJobs;
	}

	public int getCountOfCancelledJobs() {
		cancelledJobs = 0;
		for (IndexerJobStatus jobStatus : jobManagerService.getJobStatuses()) {
			if (jobStatus.getJobStatus().equals(IndexerJobStatusEnum.Cancelled)){
				cancelledJobs++;
			}
		}
		return cancelledJobs;
	}

}
