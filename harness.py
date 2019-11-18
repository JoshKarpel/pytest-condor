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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import os
import subprocess
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
            (isinstance(other, self.__class__) or isinstance(self, other.__class__))
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

    def __repr__(self):
        return "{}.{}".format(self.__class__.__name__, self.name)

    __str__ = __repr__


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

    def _fmt_value(self):
        """Some values can have special formatting depending on the attribute."""
        if self.attribute in ("JobStatus", "LastJobStatus"):
            return str(JobStatus(self.value))

        return str(self.value)

    def __repr__(self):
        return "{}(attribute = {}, value = {})".format(
            self.__class__.__name__, self.attribute, self._fmt_value()
        )

    def __str__(self):
        return "Set {} = {}".format(self.attribute, self._fmt_value())


def SetJobStatus(new_status):
    return SetAttribute("JobStatus", new_status)


DEFAULT_PARAMS = {
    "LOCAL_CONFIG_FILE": "",
    "CONDOR_HOST": "$(IP_ADDRESS)",
    "COLLECTOR_HOST": "$(CONDOR_HOST):0",
    "MASTER_ADDRESS_FILE": "$(LOG)/.master_address",
    "COLLECTOR_ADDRESS_FILE": "$(LOG)/.collector_address",
    "SCHEDD_ADDRESS_FILE": "$(LOG)/.schedd_address",
    "UPDATE_INTERVAL": "2",
    "POLLING_INTERVAL": "2",
    "NEGOTIATOR_INTERVAL": "2",
    "STARTER_UPDATE_INTERVAL": "2",
    "STARTER_INITIAL_UPDATE_INTERVAL": "2",
    "NEGOTIATOR_CYCLE_DELAY": "2",
    "MachineMaxVacateTime": "2",
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

        self.job_queue = JobQueue(self)

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

            who = self.run_command(["condor_who", "-quick"], echo=False, suppress=True)

            if who.stdout.strip() == "":
                logger.warning(
                    "condor_who stdout was unexpectedly blank for {}, retrying (giving up in {} seconds)".format(
                        self, time_to_give_up
                    )
                )
                continue

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

    def run_command(self, *args, **kwargs):
        with SetCondorConfig(self.config_file):
            return run_command(*args, **kwargs)

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


class JobQueue:
    def __init__(self, condor: Condor):
        self.condor = condor

        self.events = []
        self.by_jobid = collections.defaultdict(list)

        self._job_queue_log_file = None

    def __iter__(self):
        yield from self.events

    def filter(self, condition):
        yield from ((j, e) for j, e in self if condition(j, e))

    def read_events(self) -> Iterator[Tuple[JobID, Any]]:
        if self._job_queue_log_file is None:
            self._job_queue_log_file = self.condor.job_queue_log.open(mode="r")

        for line in self._job_queue_log_file:
            x = parse_job_queue_log_line(line)
            if x is not None:
                jobid, event = x
                self.events.append((jobid, event))
                self.by_jobid[jobid].append(event)
                yield x

    def wait(self, expected_events, unexpected_events=None, timeout: int = 60):
        all_good = True

        if unexpected_events is None:
            unexpected_events = {}

        unexpected_events = {
            jobid: set(events) for jobid, events in unexpected_events.items()
        }

        expected_events = {
            jobid: collections.deque(
                event if isinstance(event, tuple) else (event, lambda *_: None)
                for event in events
            )
            for jobid, events in expected_events.items()
        }
        expected_event_sets = {
            jobid: set(e for e, _ in events)
            for jobid, events in expected_events.items()
        }

        start = time.time()

        while True:
            elapsed = time.time() - start
            if elapsed > timeout:
                # TODO: add more info in error here
                return False

            for jobid, event in self.read_events():
                if event in expected_event_sets.get(jobid, ()):
                    expected_events_for_jobid = expected_events[jobid]

                    next_event, callback = expected_events_for_jobid[0]

                    if event == next_event:
                        expected_events_for_jobid.popleft()
                        logger.debug(
                            "Saw expected job queue event for job {}: {}".format(
                                jobid, event
                            )
                        )
                        callback(jobid, event)
                    else:
                        all_good = False
                        expected_events.pop(jobid)
                        expected_event_sets.pop(jobid)

                    if len(expected_events_for_jobid) == 0:
                        expected_events.pop(jobid)
                        expected_event_sets.pop(jobid)

                        logger.debug(
                            "Have seen all expected job queue events for job {}".format(
                                jobid
                            )
                        )

                        # if no more expected event, we're done!
                        if len(expected_events) == 0:
                            return all_good
                    else:
                        logger.debug(
                            "Still expecting job queue events for job {}: [{}]".format(
                                jobid, ", ".join(map(str, expected_events_for_jobid))
                            )
                        )
                elif event in unexpected_events.get(jobid, ()):
                    logger.error(
                        "Saw unexpected job queue event for job {}: {} (was expecting {})".format(
                            jobid, event, expected_events[jobid][0]
                        )
                    )
            time.sleep(0.1)


def run_command(
    args: List[str], stdin=None, timeout: int = 60, echo=True, suppress=False
):
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

    msg_lines = ["Ran command: {}".format(" ".join(p.args))]
    if not suppress:
        msg_lines += [
            "CONDOR_CONFIG = {}".format(os.environ.get("CONDOR_CONFIG", "<not set>")),
            "exit code: {}".format(p.returncode),
            "stdout:{}{}".format("\n" if "\n" in p.stdout else " ", p.stdout),
            "stderr:{}{}".format("\n" if "\n" in p.stderr else " ", p.stderr),
        ]

    msg = "\n".join(msg_lines)
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


_in_order_sentinel = object()


def in_order(iterable, expected):
    iterable = iter(iterable)
    iterable, backup = itertools.tee(iterable)

    expected = list(expected)
    expected_set = set(expected)
    next_expected_idx = 0

    found_at = {}

    for found_idx, item in enumerate(iterable):
        if item not in expected_set:
            continue

        if item == expected[next_expected_idx]:
            found_at[found_idx] = expected[next_expected_idx]
            next_expected_idx += 1

            if next_expected_idx == len(expected):
                # we have seen all the expected events
                return True
        else:
            break

    backup = list(backup)
    msg_lines = ["Items were not in the correct order:"]
    for idx, item in enumerate(backup):
        msg = " {:6}  {}".format(idx, item)

        maybe_found = found_at.get(idx)
        if maybe_found is not None:
            msg += "  MATCHED  {}".format(maybe_found)

        msg_lines.append(msg)

    logger.error("\n".join(msg_lines))
    return False
