
from os.path import abspath, basename, exists, join
from uuid import uuid4

import os
import math
import sys

from seisflows.tools import unix
from seisflows.tools.code import call, findpath
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class chinook_lg(custom_import('system', 'slurm_lg')):
    """ System interface for University of Alaska Fairbanks CHINOOK

      For more informations, see 
      http://seisflows.readthedocs.org/en/latest/manual/manual.html#system-interfaces
    """

    def check(self):
        """ Checks parameters and paths
        """

        # check parameters
        if 'TITLE' not in PAR:
            setattr(PAR, 'TITLE', basename(abspath('.')))

        if 'WALLTIME' not in PAR:
            setattr(PAR, 'WALLTIME', 30.)

        if 'STEPTIME' not in PAR:
            setattr(PAR, 'STEPTIME', 30.)

        if 'SLEEPTIME' not in PAR:
            setattr(PAR, 'SLEEPTIME', 1.)

        if 'VERBOSE' not in PAR:
            setattr(PAR, 'VERBOSE', 1)

        if 'NTASK' not in PAR:
            raise ParameterError(PAR, 'NTASK')

        if 'NPROC' not in PAR:
            raise ParameterError(PAR, 'NPROC')

        if 'NODESIZE' not in PAR:
            setattr(PAR, 'NODESIZE', 24)

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

        if 'SYSTEM' not in PATH:
            setattr(PATH, 'SYSTEM', join(PATH.SCRATCH, 'system'))


    def submit(self, workflow):
        """ Submits workflow
        """
        unix.cd(PATH.SUBMIT)
        if not exists('./scratch'): 
            unix.ln(PATH.SCRATCH, PATH.SUBMIT+'/'+'scratch')

        unix.mkdir(PATH.OUTPUT)
        unix.cd(PATH.OUTPUT)
        unix.mkdir(PATH.SUBMIT+'/'+'output.slurm')

        self.checkpoint()

        # prepare sbatch arguments
        call('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--partition=%s ' % 't1small'
                + '--job-name=%s ' % PAR.TITLE
                + '--output %s ' % (PATH.SUBMIT+'/'+'output.log')
                + '--ntasks-per-node=%d ' % PAR.NODESIZE
                + '--nodes=%d ' % 1
                + '--time=%d ' % PAR.WALLTIME
                + findpath('seisflows.system') +'/'+ 'wrappers/submit '
                + PATH.OUTPUT)


    def job_array_cmd(self, classname, method, hosts):

        nodes_per_job = math.ceil(PAR.NPROC/float(PAR.NODESIZE))
        if nodes_per_job <= 2:
            partition = 't1small'
        else:
            partition = 't1standard'

        return ('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--partition=%s ' % partition
                + '--job-name=%s ' % PAR.TITLE
                + '--nodes=%d ' % nodes_per_job
                + '--ntasks-per-node=%d ' % PAR.NODESIZE
                + '--ntasks=%d ' % PAR.NPROC
                + '--time=%d ' % PAR.STEPTIME
                + self.job_array_args(hosts)
                + findpath('seisflows.system') +'/'+ 'wrappers/run '
                + PATH.OUTPUT + ' '
                + classname + ' '
                + method + ' ')


    def mpiexec(self):
        """ Specifies MPI exectuable; used to invoke solver
        """
        return 'mpirun -np %d ' % PAR.NPROC

