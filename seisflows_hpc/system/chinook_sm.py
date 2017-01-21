
from os.path import exists
from uuid import uuid4

import os
import sys

from os.path import abspath, basename, join
from seisflows.tools import unix
from seisflows.tools.code import call, findpath, saveobj
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class chinook_sm(custom_import('system', 'slurm_sm')):
    """ System interface for University of Alaska Fairbanks CHINOOK

      For important additional information, please see 
      http://seisflows.readthedocs.org/en/latest/manual/manual.html#system-configuration
    """


    def check(self):
        """ Checks parameters and paths
        """

        # check parameters
        if 'TITLE' not in PAR:
            setattr(PAR, 'TITLE', basename(abspath('.')))

        if 'WALLTIME' not in PAR:
            setattr(PAR, 'WALLTIME', 30.)

        if 'VERBOSE' not in PAR:
            setattr(PAR, 'VERBOSE', 1)

        if 'NPROC' not in PAR:
            raise ParameterError(PAR, 'NPROC')

        if 'NTASK' not in PAR:
            raise ParameterError(PAR, 'NTASK')

        if 'SLURMARGS' not in PAR:
            setattr(PAR, 'SLURMARGS', '')

        # check paths
        if 'SCRATCH' not in PATH:
            setattr(PATH, 'SCRATCH', join(os.getenv('CENTER'), 'scratch', str(uuid4())))

        if 'LOCAL' not in PATH:
            setattr(PATH, 'LOCAL', None)

        if 'SUBMIT' not in PATH:
            setattr(PATH, 'SUBMIT', abspath('.'))

        if 'OUTPUT' not in PATH:
            setattr(PATH, 'OUTPUT', join(PATH.SUBMIT, 'output'))


    def submit(self, workflow):
        """ Submits workflow
        """
        unix.cd(PATH.SUBMIT)
        if not exists('./scratch'): 
            unix.ln(PATH.SCRATCH, PATH.SUBMIT+'/'+'scratch')

        unix.mkdir(PATH.OUTPUT)
        self.checkpoint()

        # prepare sbatch arguments
        call('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--partition=%s ' % 't1small'
                + '--job-name=%s ' % PAR.TITLE
                + '--output %s ' % (PATH.SUBMIT+'/'+'output.log')
                + '--cpus-per-task=%d '%PAR.NPROC
                + '--ntasks=%d '%PAR.NTASK
                + '--time=%d ' % PAR.WALLTIME
                + findpath('seisflows.system') +'/'+ 'wrappers/submit '
                + PATH.OUTPUT)

