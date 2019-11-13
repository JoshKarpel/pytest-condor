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

from typing import List, Dict, Mapping, Optional

import logging

logging.basicConfig(format = "[%(levelname)s] %(asctime)s ~ %(message)s", level = logging.DEBUG)

logger = logging.getLogger(__name__)

import os
import subprocess
import sys
from pathlib import Path
import shutil
import time

DEFAULT_PARAMS = {
    'LOCAL_CONFIG_FILE': '',
    'CONDOR_HOST': "$(IP_ADDRESS)",
    'COLLECTOR_HOST': "$(CONDOR_HOST):0",
    'MASTER_ADDRESS_FILE': "$(LOG)/.master_address",
    'COLLECTOR_ADDRESS_FILE': "$(LOG)/.collector_address",
    'SCHEDD_ADDRESS_FILE': "$(LOG)/.schedd_address",
    "UPDATE_INTERVAL": 5,
    "POLLING_INTERVAL": 5,
    "NEGOTIATOR_INTERVAL": 5,
    "STARTER_UPDATE_INTERVAL": 5,
    "STARTER_INITIAL_UPDATE_INTERVAL": 5,
    "NEGOTIATOR_CYCLE_DELAY": 5,
    "MachineMaxVacateTime": 5,
    "RUNBENCHMARKS": 0
}


class Condor:
    def __init__(self, release_dir: Path, local_dir: Path, params: Mapping[str, str] = None, raw_params: str = None):
        self.release_dir = release_dir

        self.local_dir = local_dir

        self.execute_dir = self.local_dir / 'execute'
        self.lock_dir = self.local_dir / 'lock'
        self.log_dir = self.local_dir / 'log'
        self.run_dir = self.local_dir / 'run'
        self.spool_dir = self.local_dir / 'spool'

        self.config_file = self.local_dir / 'condor_config'

        if params is None:
            params = {}
        self.params = {k: v if v is not None else '' for k, v in params}
        self.raw_params = raw_params or ''

    def __enter__(self):
        self._start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

    def _start(self):
        logger.info('Starting {}'.format(self))

        self._setup_local_dirs()
        self._write_config()
        with SetCondorConfig(self.config_file):
            self._start_condor()
            self._wait_for_ready()

        logger.info('Started {}'.format(self))

    def _setup_local_dirs(self):
        for dir in (self.local_dir, self.execute_dir, self.lock_dir, self.log_dir, self.run_dir, self.spool_dir):
            dir.mkdir(parents = True, exist_ok = False)
            logger.debug("Created dir {}".format(dir))

    def _write_config(self):
        run_command(['condor_config_val', '-write:up', self.config_file.as_posix()])

        param_lines = []

        param_lines += ["#", "# ROLES", "#"]
        param_lines += [
            "use ROLE: CentralManager",
            "use ROLE: Submit",
            "use ROLE: Execute",
        ]

        base_config = {
            'RELEASE_DIR': self.release_dir.as_posix(),
            'LOCAL_DIR': self.local_dir.as_posix(),
            'EXECUTE': self.execute_dir.as_posix(),
            'LOCK': self.lock_dir.as_posix(),
            'LOG': self.log_dir.as_posix(),
            'RUN': self.run_dir.as_posix(),
            'SPOOL': self.spool_dir.as_posix(),
        }

        param_lines += ['#', '# BASE PARAMS', '#']
        param_lines += ['{} = {}'.format(k, v) for k, v in base_config.items()]

        param_lines += ['#', '# DEFAULT PARAMS', '#']
        param_lines += ['{} = {}'.format(k, v) for k, v in DEFAULT_PARAMS.items()]

        param_lines += ['#', '# CUSTOM PARAMS', '#']
        param_lines += ['{} = {}'.format(k, v) for k, v in self.params.items()]

        param_lines += ['#', '# RAW PARAMS', '#']
        param_lines += self.raw_params.splitlines()

        # self.config_file.write_text('\n'.join(param_lines))
        with self.config_file.open(mode = 'a') as f:
            f.write('\n'.join(param_lines))
        logger.debug("Wrote config file for {} to {}".format(self, self.config_file))

    def _start_condor(self):
        self.condor_master = subprocess.Popen(
            ['condor_master'],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
        )

        logger.debug("condor_master started as pid {}".format(self.condor_master.pid))

    def _wait_for_ready(self, timeout = 120, dump_logs_if_fail = False):
        daemons = self.run_command(["condor_config_val", 'DAEMON_LIST'], echo = False).stdout
        logger.debug("Waiting for daemons to start up: {}".format(daemons))
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(5)
            who = self.run_command(['condor_who', '-quick'])

            if 'IsReady = true' in who.stdout:
                return

        logger.error("Failed to start all daemons.")
        if dump_logs_if_fail:
            for logfile in self.log_dir.iterdir():
                logger.error("Contents of {}:\n{}".format(logfile, logfile.read_text()))

        raise TimeoutError("Daemon startup failed")

    def _cleanup(self):
        logger.info('Cleaning up {}'.format(self))

        with SetCondorConfig(self.config_file):
            self._condor_off()
            self._wait_for_master_to_terminate()
            # self._remove_local_dir()

        logger.info('Cleaned up {}'.format(self))

    def _condor_off(self):
        off = self.run_command(["condor_off", "-daemon", "master"], timeout = 30)

        if not off.returncode == 0:
            logger.error("condor_off failed, exit code: {}, stderr: {}".format(off.returncode, off.stderr))
            self._terminate_condor_master()
            return

        logger.debug("condor_off succeeded: {}".format(off.stdout))

    def _wait_for_master_to_terminate(self, kill_after = 30, timeout = 60):
        logger.debug("Waiting for condor_master (pid {}) to terminate".format(self.condor_master.pid))

        start = time.time()
        killed = False
        while True:
            if self.condor_master.poll() is not None:
                break

            elapsed = time.time() - start

            if not killed:
                logger.debug("condor_master has not terminated yet, will kill in {} seconds".format(int(kill_after - elapsed)))

            if elapsed > kill_after and not killed:
                self._kill_condor_master()
                killed = True

            if elapsed > timeout:
                raise TimeoutError("Timed out while waiting for condor_master to terminate")

            time.sleep(1)

        logger.debug("condor_master (pid {}) terminated with exit code {}".format(self.condor_master.pid, self.condor_master.returncode))

    def _terminate_condor_master(self):
        self.condor_master.terminate()
        logger.debug("Sent terminate signal to condor_master (pid {})".format(self.condor_master.pid))

    def _kill_condor_master(self):
        self.condor_master.kill()
        logger.debug("Sent kill signal to condor_master (pid {})".format(self.condor_master.pid))

    # def _remove_local_dir(self):
    #     shutil.rmtree(self.local_dir)
    #     logger.debug("Removed local dir {}".format(self.local_dir))

    def read_params(self):
        return self.config_file.read_text()

    def run_command(self, args: List[str], timeout: Optional[int] = None, echo = True):
        with SetCondorConfig(self.config_file):
            return run_command(args, timeout = timeout, echo = echo)


def run_command(args: List[str], timeout: Optional[int] = None, echo = True):
    p = subprocess.run(args, timeout = timeout, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    p.stdout = p.stdout.decode("utf-8").strip()
    p.stderr = p.stderr.decode("utf-8").strip()

    if echo:
        print('Ran command {}'.format(p.args))
        print('exit code: {}'.format(p.returncode))
        print('stdout:\n{}'.format(p.stdout))
        print('stderr:\n{}'.format(p.stderr))

    return p


def set_env_var(key, value):
    os.environ[key] = value
    logger.debug("Set environment variable {} = {}".format(key, value))


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


###############################

def test_foo():
    shutil.rmtree(Path.home() / 'condor', ignore_errors = True)
    with Condor(release_dir = Path('/usr'), local_dir = Path.home() / 'condor') as condor:
        condor.run_command(args = ['condor_version'], timeout = 60)
        condor.run_command(args = ['condor_status'], timeout = 60)


if __name__ == "__main__":
    test_foo()
