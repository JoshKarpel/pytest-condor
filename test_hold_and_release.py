#!/usr/bin/env pytest

# this test replicates job_core_holdrelease_van

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import pytest

from harness import (
    write_file,
    get_submit_result,
    JobID,
    SetAttribute,
    SetJobStatus,
    JobStatus,
    in_order,
)


@pytest.fixture(scope="class")
def job_queue_events_for_sleep_job(default_condor, test_dir):
    sub_description = """
        executable = /bin/sleep
        arguments = 10
        
        queue
    """
    submit_file = write_file(test_dir / "submit" / "job.sub", sub_description)

    submit_cmd = default_condor.run_command(["condor_submit", submit_file])
    clusterid, num_procs = get_submit_result(submit_cmd)
    jobid = JobID(clusterid, 0)

    default_condor.job_queue.wait(
        {
            jobid: [
                (  # when the job starts running, hold it
                    SetJobStatus(JobStatus.Running),
                    lambda jobid, event: default_condor.run_command(
                        ["condor_hold", jobid]
                    ),
                ),
                (  # once the job is held, release it
                    SetJobStatus(JobStatus.Held),
                    lambda jobid, event: default_condor.run_command(
                        ["condor_release", jobid]
                    ),
                ),
                SetJobStatus(JobStatus.Completed),
            ]
        },
        timeout=60,
    )

    return default_condor.job_queue.by_jobid[jobid]


class TestCanHoldAndReleaseJob:
    def test_job_queue_events_in_correct_order(self, job_queue_events_for_sleep_job):
        assert in_order(
            job_queue_events_for_sleep_job,
            [
                SetJobStatus(JobStatus.Idle),
                SetJobStatus(JobStatus.Running),
                SetJobStatus(JobStatus.Held),
                SetJobStatus(JobStatus.Idle),
                SetJobStatus(JobStatus.Running),
                SetJobStatus(JobStatus.Completed),
            ],
        )

    def test_hold_reason_code_was_1(self, job_queue_events_for_sleep_job):
        assert SetAttribute("HoldReasonCode", "1") in job_queue_events_for_sleep_job
