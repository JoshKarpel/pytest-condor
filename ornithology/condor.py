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

from typing import Mapping, Callable

import logging

import subprocess
from pathlib import Path
import shutil
import time
import functools
import shlex

import htcondor

from . import job_queue, env, cmd, daemon


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
    "MAX_JOB_QUEUE_LOG_ROTATIONS": "0",
    "STARTER_LIST": "STARTER",  # no standard universe starter
}


def master_is_not_alive(self):
    return not self.master_is_alive


def condor_is_ready(self):
    return self.condor_is_ready


def condor_master_was_started(self):
    return self.condor_master is not None


def skip_if(condition):
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
        self, local_dir: Path, config=None, raw_config=None, clean_local_dir_before=True
    ):
        self.local_dir = local_dir

        self.execute_dir = self.local_dir / "execute"
        self.lock_dir = self.local_dir / "lock"
        self.log_dir = self.local_dir / "log"
        self.run_dir = self.local_dir / "run"
        self.spool_dir = self.local_dir / "spool"
        self.passwords_dir = self.local_dir / "passwords.d"
        self.tokens_dir = self.local_dir / "tokens.d"

        self.config_file = self.local_dir / "condor_config"

        if config is None:
            config = {}
        self.config = {k: v if v is not None else "" for k, v in config.items()}
        self.raw_config = raw_config or ""

        self.clean_local_dir_before = clean_local_dir_before

        self.condor_master = None
        self.condor_is_ready = False

        self.job_queue = job_queue.JobQueue(self)

    def use_config(self):
        return env.SetCondorConfig(self.config_file)

    def __repr__(self):
        return "{}(local_dir = {})".format(self.__class__.__name__, self.local_dir)

    @property
    def master_is_alive(self):
        return self.condor_master is not None and self.condor_master.poll() is None

    def __enter__(self):
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
            self.passwords_dir,
            self.tokens_dir,
        ):
            dir.mkdir(parents=True, exist_ok=False)
            logger.debug("Created dir {}".format(dir))

    def _write_config(self):
        # TODO: how to ensure that this always hits the right config?
        # TODO: switch to -summary instead of -write:up
        write = cmd.run_command(
            ["condor_config_val", "-write:up", self.config_file.as_posix()],
            echo=False,
            suppress=True,
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
            "SEC_PASSWORD_DIRECTORY": self.passwords_dir.as_posix(),
            "SEC_TOKEN_SYSTEM_DIRECTORY": self.tokens_dir.as_posix(),
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
        with env.SetCondorConfig(self.config_file):
            self.condor_master = subprocess.Popen(
                ["condor_master", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            logger.debug(
                "Started condor_master (pid {})".format(self.condor_master.pid)
            )

    @skip_if(condor_is_ready)
    def _wait_for_ready(self, timeout=120, dump_logs_if_fail=False):
        daemons = set(
            self.run_command(
                ["condor_config_val", "DAEMON_LIST"], echo=False
            ).stdout.split(" ")
        )

        logger.debug(
            "Starting up daemons for {}, waiting for: {}".format(
                self, " ".join(sorted(daemons))
            )
        )

        start = time.time()
        while time.time() - start < timeout:
            time_to_give_up = int(timeout - (time.time() - start))

            # if the master log does not exist yet, we can't use condor_who
            if not self.master_log.path.exists():
                logger.debug(
                    "MASTER_LOG at {} does not yet exist for {} (giving up in {} seconds)".format(
                        self.master_log, self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who = self.run_command(
                shlex.split(
                    "condor_who -wait:10 'IsReady && STARTD_State =?= \"Ready\"'"
                ),
                echo=False,
                suppress=True,
            )
            if who.stdout.strip() == "":
                logger.warning(
                    "condor_who stdout was unexpectedly blank for {}, retrying (giving up in {} seconds)".format(
                        self, time_to_give_up
                    )
                )
                time.sleep(1)
                continue

            who_ad = dict(kv.split(" = ") for kv in who.stdout.splitlines())

            if (
                who_ad.get("IsReady") == "true"
                and who_ad.get("STARTD_State") == '"Ready"'
                and all(who_ad.get(d) == '"Alive"' for d in daemons)
            ):
                self.condor_is_ready = True
                return

            logger.debug(
                "{} is waiting for daemons to be ready (giving up in {} seconds)".format(
                    self, time_to_give_up
                )
            )

        self.run_command(["condor_who", "-quick"])
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
            try:
                self.condor_master.communicate(timeout=5)
                break
            except TimeoutError:
                pass

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

    def read_config(self):
        return self.config_file.read_text()

    def run_command(self, *args, **kwargs):
        with self.use_config():
            return cmd.run_command(*args, **kwargs)

    @property
    def master_log(self) -> daemon.DaemonLog:
        return self._get_daemon_log("MASTER")

    @property
    def collector_log(self) -> daemon.DaemonLog:
        return self._get_daemon_log("COLLECTOR")

    @property
    def negotiator_log(self) -> daemon.DaemonLog:
        return self._get_daemon_log("NEGOTIATOR")

    @property
    def schedd_log(self) -> daemon.DaemonLog:
        return self._get_daemon_log("SCHEDD")

    @property
    def startd_log(self) -> daemon.DaemonLog:
        return self._get_daemon_log("STARTD")

    @property
    def job_queue_log(self) -> Path:
        return self._get_log_path("JOB_QUEUE")

    def _get_log_path(self, log_type) -> Path:
        p = self.run_command(
            ["condor_config_val", "{}_LOG".format(log_type)], echo=False, suppress=True
        ).stdout
        return Path(p)

    def _get_daemon_log(self, daemon_name):
        return daemon.DaemonLog(self._get_log_path(daemon_name))

    def status(self, ad_type=htcondor.AdTypes.Any, constraint="true", projection=None):
        projection = projection or []

        with self.use_config():
            coll = htcondor.Collector()
            return coll.query(
                ad_type=ad_type, constraint=constraint, projection=projection
            )

    def q(
        self,
        constraint="true",
        projection=None,
        limit=None,
        opts=htcondor.QueryOpts.Default,
    ):
        if limit is None:
            limit = -1

        with self.use_config():
            schedd = htcondor.Schedd()
            return schedd.query(
                constraint=constraint, attr_list=projection, limit=limit, opts=opts
            )
