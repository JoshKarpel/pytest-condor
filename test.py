# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Dict, Mapping, Optional, Callable, Iterator, Tuple, Any

import logging

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s ~ %(message)s", level=logging.DEBUG
)

logger = logging.getLogger(__name__)

import os
import subprocess
import sys
from pathlib import Path
import shutil
import time
import functools
import itertools
import textwrap
import collections
import re
import enum
import inspect


class JobID:
    def __init__(self, cluster, proc):
        self.cluster = int(cluster)
        self.proc = int(proc)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.cluster == other.cluster
            and self.proc == other.proc
        )

    def __hash__(self):
        return hash((self.__class__, self.cluster, self.proc))

    def __repr__(self):
        return "{}(cluster = {}, proc = {})".format(
            self.__class__.__name__, self.cluster, self.proc
        )

    def __str__(self):
        return "{}.{}".format(self.cluster, self.proc)


class JobStatus(str, enum.Enum):
    Idle = "1"
    Running = "2"
    Removed = "3"
    Completed = "4"
    Held = "5"
    TransferringOutput = "6"
    Suspended = "7"


class SetAttribute:
    def __init__(self, attribute, value):
        self.attribute = attribute
        self.value = value

    def __eq__(self, other):
        return (
            (isinstance(other, self.__class__) or isinstance(self, other.__class__))
            and self.attribute == other.attribute
            and self.value == other.value
        )

    def __hash__(self):
        return hash((self.__class__, self.attribute, self.value))

    def __repr__(self):
        return "{}(attribute = {}, value = {})".format(
            self.__class__.__name__, self.attribute, self.value
        )

    def __str__(self):
        return "Set {} = {}".format(self.attribute, self.value)


def SetJobStatus(new_status):
    return SetAttribute("JobStatus", new_status)


DEFAULT_PARAMS = {
    "LOCAL_CONFIG_FILE": "",
    "CONDOR_HOST": "$(IP_ADDRESS)",
    "COLLECTOR_HOST": "$(CONDOR_HOST):0",
    "MASTER_ADDRESS_FILE": "$(LOG)/.master_address",
    "COLLECTOR_ADDRESS_FILE": "$(LOG)/.collector_address",
    "SCHEDD_ADDRESS_FILE": "$(LOG)/.schedd_address",
    "UPDATE_INTERVAL": 5,
    "POLLING_INTERVAL": 5,
    "NEGOTIATOR_INTERVAL": 5,
    "STARTER_UPDATE_INTERVAL": 5,
    "STARTER_INITIAL_UPDATE_INTERVAL": 5,
    "NEGOTIATOR_CYCLE_DELAY": 5,
    "MachineMaxVacateTime": 5,
    "RUNBENCHMARKS": "False",
    "JOB_QUEUE_LOG": "$(SPOOL)/job_queue.log",
}


def master_is_not_alive(self) -> bool:
    return not self.master_is_alive


def condor_is_ready(self) -> bool:
    return self.condor_is_ready


def condor_master_was_started(self) -> bool:
    return self.condor_master is not None


def skip_if(condition: Callable[["Condor"], bool]):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if condition(self):
                logger.debug(
                    "Skipping call to {} for {} because {} was True".format(
                        func.__name__, self, condition.__name__
                    )
                )
                return

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


class Condor:
    def __init__(
        self,
        local_dir: Path,
        config: Mapping[str, str] = None,
        raw_config: str = None,
        clean_local_dir_before: bool = True,
    ):
        self.local_dir = local_dir

        self.execute_dir = self.local_dir / "execute"
        self.lock_dir = self.local_dir / "lock"
        self.log_dir = self.local_dir / "log"
        self.run_dir = self.local_dir / "run"
        self.spool_dir = self.local_dir / "spool"

        self.config_file = self.local_dir / "condor_config"

        if config is None:
            config = {}
        self.config = {k: v if v is not None else "" for k, v in config.items()}
        self.raw_config = raw_config or ""

        self.clean_local_dir_before = clean_local_dir_before

        self.condor_master = None
        self.condor_is_ready = False

        self._job_queue_log_file = None
        self._jobid_to_events = collections.defaultdict(list)

    def __repr__(self):
        return "{}(local_dir = {})".format(self.__class__.__name__, self.local_dir)

    @property
    def master_is_alive(self) -> bool:
        return self.condor_master is not None and self.condor_master.poll() is None

    def __enter__(self) -> "Condor":
        self._start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

    def _start(self):
        logger.info("Starting {}".format(self))

        try:
            self._setup_local_dirs()
            self._write_config()
            self._start_condor()
            self._wait_for_ready()
        except BaseException:
            logger.exception(
                "Encountered error during setup of {}, cleaning up!".format(self)
            )
            self._cleanup()
            raise

        logger.info("Started {}".format(self))

    def _setup_local_dirs(self):
        if self.clean_local_dir_before and self.local_dir.exists():
            shutil.rmtree(self.local_dir)
            logger.debug("Removed existing local dir for {}".format(self))

        for dir in (
            self.local_dir,
            self.execute_dir,
            self.lock_dir,
            self.log_dir,
            self.run_dir,
            self.spool_dir,
        ):
            dir.mkdir(parents=True, exist_ok=False)
            logger.debug("Created dir {}".format(dir))

    def _write_config(self):
        # TODO: how to ensure that this always hits the right config?
        # TODO: switch to -summary instead of -write:up
        write = run_command(
            ["condor_config_val", "-write:up", self.config_file.as_posix()], echo=False
        )
        if write.returncode != 0:
            raise Exception("Failed to copy base OS config: {}".format(write.stderr))

        param_lines = []

        param_lines += ["#", "# ROLES", "#"]
        param_lines += [
            "use ROLE: CentralManager",
            "use ROLE: Submit",
            "use ROLE: Execute",
        ]

        base_config = {
            "LOCAL_DIR": self.local_dir.as_posix(),
            "EXECUTE": self.execute_dir.as_posix(),
            "LOCK": self.lock_dir.as_posix(),
            "LOG": self.log_dir.as_posix(),
            "RUN": self.run_dir.as_posix(),
            "SPOOL": self.spool_dir.as_posix(),
            "STARTD_DEBUG": "D_FULLDEBUG D_COMMAND",
        }

        param_lines += ["#", "# BASE PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in base_config.items()]

        param_lines += ["#", "# DEFAULT PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in DEFAULT_PARAMS.items()]

        param_lines += ["#", "# CUSTOM PARAMS", "#"]
        param_lines += ["{} = {}".format(k, v) for k, v in self.config.items()]

        param_lines += ["#", "# RAW PARAMS", "#"]
        param_lines += self.raw_config.splitlines()

        with self.config_file.open(mode="a") as f:
            f.write("\n".join(param_lines))
        logger.debug("Wrote config file for {} to {}".format(self, self.config_file))

    @skip_if(condor_master_was_started)
    def _start_condor(self):
        with SetCondorConfig(self.config_file):
            self.condor_master = subprocess.Popen(
                ["condor_master", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            logger.debug(
                "Started condor_master (pid {})".format(self.condor_master.pid)
            )

    @skip_if(condor_is_ready)
    def _wait_for_ready(self, timeout=120, check_delay=10, dump_logs_if_fail=False):
        unready_daemons = set(
            self.run_command(
                ["condor_config_val", "DAEMON_LIST"], echo=False
            ).stdout.split(" ")
        )
        logger.debug(
            "Starting up daemons for {}, waiting for: {}".format(
                self, " ".join(sorted(unready_daemons))
            )
        )

        start = time.time()
        while time.time() - start < timeout:
            time.sleep(check_delay)
            time_to_give_up = int(timeout - (time.time() - start))

            # if the master log does not exist yet, we can't use condor_who
            if not self.master_log.exists():
                logger.debug(
                    "MASTER_LOG at {} does not yet exist for {} (giving up in {} seconds)".format(
                        self.master_log, self, time_to_give_up
                    )
                )
                continue

            who = self.run_command(["condor_who", "-quick"], echo=False)
            who_ad = dict(kv.split(" = ") for kv in who.stdout.splitlines())

            if who_ad.get("IsReady") == "true":
                self.condor_is_ready = True
                return

            for k, v in who_ad.items():
                if v == '"Alive"':
                    unready_daemons.discard(k)

            logger.debug(
                "{} is waiting for daemons to be ready (giving up in {} seconds): {}".format(
                    self, time_to_give_up, " ".join(sorted(unready_daemons))
                )
            )
            for d in sorted(unready_daemons):
                logger.debug(
                    "Status of Daemon {} (pid {}) for {}: {}".format(
                        d, who_ad[d + "_PID"], self, who_ad[d]
                    )
                )

        logger.error("Failed to start daemons: ")
        if dump_logs_if_fail:
            for logfile in self.log_dir.iterdir():
                logger.error("Contents of {}:\n{}".format(logfile, logfile.read_text()))

        raise TimeoutError("Standup for {} failed".format(self))

    def _cleanup(self):
        logger.info("Cleaning up {}".format(self))

        self._condor_off()
        self._wait_for_master_to_terminate()
        # TODO: look for core dumps
        # self._remove_local_dir()

        logger.info("Cleaned up {}".format(self))

    @skip_if(master_is_not_alive)
    def _condor_off(self):
        off = self.run_command(
            ["condor_off", "-daemon", "master"], timeout=30, echo=False
        )

        if not off.returncode == 0:
            logger.error(
                "condor_off failed, exit code: {}, stderr: {}".format(
                    off.returncode, off.stderr
                )
            )
            self._terminate_condor_master()
            return

        logger.debug("condor_off succeeded: {}".format(off.stdout))

    @skip_if(master_is_not_alive)
    def _wait_for_master_to_terminate(self, kill_after=60, timeout=120):
        logger.debug(
            "Waiting for condor_master (pid {}) to terminate".format(
                self.condor_master.pid
            )
        )

        start = time.time()
        killed = False
        while True:
            if self.condor_master.poll() is not None:
                break

            elapsed = time.time() - start

            if not killed:
                logger.debug(
                    "condor_master has not terminated yet, will kill in {} seconds".format(
                        int(kill_after - elapsed)
                    )
                )

            if elapsed > kill_after and not killed:
                # TODO: in this path, we should also kill the other daemons
                # TODO: we can find their pids by reading the master log
                self._kill_condor_master()
                killed = True

            if elapsed > timeout:
                raise TimeoutError(
                    "Timed out while waiting for condor_master to terminate"
                )

            time.sleep(5)

        logger.debug(
            "condor_master (pid {}) has terminated with exit code {}".format(
                self.condor_master.pid, self.condor_master.returncode
            )
        )

    @skip_if(master_is_not_alive)
    def _terminate_condor_master(self):
        if not self.master_is_alive:
            return

        self.condor_master.terminate()
        logger.debug(
            "Sent terminate signal to condor_master (pid {})".format(
                self.condor_master.pid
            )
        )

    @skip_if(master_is_not_alive)
    def _kill_condor_master(self):
        self.condor_master.kill()
        logger.debug(
            "Sent kill signal to condor_master (pid {})".format(self.condor_master.pid)
        )

    # def _remove_local_dir(self):
    #     shutil.rmtree(self.local_dir)
    #     logger.debug("Removed local dir {}".format(self.local_dir))

    def read_config(self):
        return self.config_file.read_text()

    def run_command(self, args: List[str], timeout: int = 60, echo=True):
        with SetCondorConfig(self.config_file):
            return run_command(args, timeout=timeout, echo=echo)

    @property
    def master_log(self) -> Path:
        return self._get_log_path("MASTER")

    @property
    def collector_log(self) -> Path:
        return self._get_log_path("COLLECTOR")

    @property
    def negotiator_log(self) -> Path:
        return self._get_log_path("NEGOTIATOR")

    @property
    def schedd_log(self) -> Path:
        return self._get_log_path("SCHEDD")

    @property
    def startd_log(self) -> Path:
        return self._get_log_path("STARTD")

    @property
    def job_queue_log(self) -> Path:
        return self._get_log_path("JOB_QUEUE")

    def _get_log_path(self, daemon: str) -> Path:
        p = self.run_command(
            ["condor_config_val", "{}_LOG".format(daemon)], echo=False
        ).stdout
        return Path(p)

    def read_events_from_job_queue_log(self) -> Iterator[Tuple[JobID, Any]]:
        if self._job_queue_log_file is None:
            self._job_queue_log_file = self.job_queue_log.open(mode="r")

        for line in self._job_queue_log_file:
            x = parse_job_queue_log_line(line)
            if x is not None:
                jobid, event = x
                self._jobid_to_events[jobid].append(event)
                logger.debug("Read event for jobid {}: {}".format(jobid, event))
                yield x

    def get_job_queue_events(self) -> Mapping:
        # just drive the iterator to completion
        for _ in self.read_events_from_job_queue_log():
            pass

        return self._jobid_to_events

    def wait_for_job_queue_events(self, jobid_to_expected_events, timeout: int = 60):
        jobid_to_expected_event_sets = {
            jobid: set(events) for jobid, events in jobid_to_expected_events.items()
        }
        jobid_to_expected_events = {
            jobid: collections.deque(events)
            for jobid, events in jobid_to_expected_events.items()
        }

        start = time.time()

        while True:
            elapsed = time.time() - start
            if elapsed > timeout:
                # TODO: add more info in error here
                raise TimeoutError("Timed out while waiting for job events")

            for jobid, event in self.read_events_from_job_queue_log():
                if event in jobid_to_expected_event_sets.get(jobid, ()):
                    expected_events = jobid_to_expected_events[jobid]

                    # The idea here is that, because we enforce order, if
                    # the next event is the one we just got, we proceed.
                    # If it isn't, the test has already failed, and we stop
                    # immediately.
                    assert event == expected_events[0]
                    expected_events.popleft()

                    logger.debug(
                        "Saw expected event for job {}: {}".format(jobid, event)
                    )

                    if len(expected_events) == 0:
                        jobid_to_expected_events.pop(jobid)
                        jobid_to_expected_event_sets.pop(jobid)

                        if len(jobid_to_expected_events) == 0:
                            return
                    else:
                        logger.debug(
                            "Still expecting events for job {}: [{}]".format(
                                jobid, ", ".join(map(str, expected_events))
                            )
                        )
            time.sleep(0.1)


def run_command(args: List[str], stdin=None, timeout: int = 60, echo=True):
    if timeout is None:
        raise TypeError("run_command timeout cannot be None")

    args = list(map(str, args))
    p = subprocess.run(
        args,
        timeout=timeout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=stdin,
        universal_newlines=True,
    )
    p.stdout = p.stdout.rstrip()
    p.stderr = p.stderr.rstrip()

    msg = "\n".join(
        (
            "Ran command: {}".format(" ".join(p.args)),
            "CONDOR_CONFIG = {}".format(os.environ.get("CONDOR_CONFIG", "<not set>")),
            "exit code: {}".format(p.returncode),
            "stdout:{}{}".format("\n" if "\n" in p.stdout else " ", p.stdout),
            "stderr:{}{}".format("\n" if "\n" in p.stderr else " ", p.stderr),
        )
    )
    logger.debug(msg)
    if echo:
        print(msg)

    return p


def set_env_var(key, value):
    os.environ[key] = value
    logger.debug("Set environment variable {} = {}".format(key, value))


def unset_env_var(key):
    value = os.environ.get(key, None)

    if value is not None:
        del os.environ[key]
        logger.debug("Unset environment variable {}, value was {}".format(key, value))


class SetCondorConfig:
    def __init__(self, config_file: Path):
        self.config_file = Path(config_file)
        self.previous_value = None

    def __enter__(self):
        self.previous_value = os.environ.get("CONDOR_CONFIG", None)
        set_env_var("CONDOR_CONFIG", self.config_file.as_posix())

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.previous_value is not None:
            set_env_var("CONDOR_CONFIG", self.previous_value)
        else:
            unset_env_var("CONDOR_CONFIG")


def write_file(path: Path, text: str) -> Path:
    path = Path(path).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


RE_SUBMIT_RESULT = re.compile(r"(\d+) job\(s\) submitted to cluster (\d+)\.")


def get_submit_result(submit_cmd: subprocess.CompletedProcess) -> Tuple[int, int]:
    match = RE_SUBMIT_RESULT.search(submit_cmd.stdout)
    if match is not None:
        num_procs = int(match.group(1))
        clusterid = int(match.group(2))
        return clusterid, num_procs

    raise ValueError(
        'Was not able to extract submit results from command "{}", stdout:\n{}\nstderr:\n{}'.format(
            " ".join(submit_cmd.args), submit_cmd.stdout, submit_cmd.stderr
        )
    )


def parse_job_queue_log_line(line: str):
    parts = line.strip().split(" ", 3)

    if parts[0] == "103":
        return JobID(*parts[1].split(".")), SetAttribute(parts[2], parts[3])


def get_current_func_name() -> str:
    return inspect.currentframe().f_back.f_code.co_name


###############################

BASE = Path.home() / "tests"


def test_single_job_can_be_submitted_and_finish_successfully():
    # or this file's name, or provided by the test framework, etc...
    test_dir = BASE / get_current_func_name()

    # inside the block, this Condor is alive
    with Condor(local_dir=test_dir / "condor") as condor:
        # TODO: cross-platform noop
        sub_description = """
            executable = /bin/sleep
            arguments = 1
            
            queue
        """
        submit_file = write_file(test_dir / "job.sub", sub_description)

        # Condor.run_command temporarily sets CONDOR_CONFIG to talk to that Condor
        submit_cmd = condor.run_command(["condor_submit", submit_file])

        # did the submit itself succeed?
        assert submit_cmd.returncode == 0

        clusterid, num_procs = get_submit_result(submit_cmd)

        # we intended to submit 1 job, but did we?
        assert num_procs == 1

        # assert that the given events for this job occur in the given order
        # it does not respect the lack of ordering inside job queue transactions!
        jobid = JobID(clusterid, 0)
        condor.wait_for_job_queue_events(
            {
                jobid: [
                    SetJobStatus(JobStatus.Idle),
                    SetJobStatus(JobStatus.Running),
                    SetJobStatus(JobStatus.Completed),
                ]
            }
        )

        # get all of the job queue events as a mapping of jobid -> List[events] (ordered)
        # useful for making asserts about the past
        jqe = condor.get_job_queue_events()

        # assert that the job itself exited successfully
        # have to be careful: the second argument of SetAttribute is always a string!
        assert SetAttribute("ExitCode", "0") in jqe[jobid]


# def test_dagman_basic():
#     with Condor(local_dir=BASE / "condor") as condor:
#         submit_dir = BASE / "submit"
#
#         dag_description = """
#             JOB         NODERET      basic-node.sub
#             SCRIPT PRE  NODERET     ./x_dagman_premonitor.pl
#             SCRIPT POST NODERET     ./job_dagman_monitor.pl $RETURN
#             """
#         dagfile = write_file(submit_dir / "dagfile.dag", dag_description)
#
#         dag_job = """
#             executable      = ./x_dagman_node-ret.pl
#             universe        = vanilla
#             log             = basic-node.log
#             notification    = NEVER
#             getenv          = true
#             output          = basic-node.out
#             error           = basic-node.err
#             queue
#             """
#         write_file(submit_dir / "basic-node.sub", dag_description)
#
#         submit_cmd = condor.run_command(["condor_submit_dag", dagfile])
#
#         assert submit_cmd.returncode == 0


if __name__ == "__main__":
    test_single_job_can_be_submitted_and_finish_successfully()
