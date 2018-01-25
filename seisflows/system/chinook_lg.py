
import os
import math
import sys
import time

from os.path import abspath, basename, exists, join
from subprocess import check_output
from uuid import uuid4
from seisflows.tools import unix
from seisflows.tools.tools import call, findpath
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
        # name of job
        if 'TITLE' not in PAR:
            setattr(PAR, 'TITLE', basename(abspath('.')))

        # time allocated for workflow in minutes
        if 'WALLTIME' not in PAR:
            setattr(PAR, 'WALLTIME', 30.)

        # time allocated for each individual task in minutes
        if 'TASKTIME' not in PAR:
            setattr(PAR, 'TASKTIME', 15.)

        # number of tasks
        if 'NTASK' not in PAR:
            raise ParameterError(PAR, 100)

        # number of cores per task
        if 'NPROC' not in PAR:
            raise ParameterError(PAR, 'NPROC')

        # limit on number of concurrent tasks
        if 'NTASKMAX' not in PAR:
            setattr(PAR, 'NTASKMAX', PAR.NTASK)

        # number of cores per node
        if 'NODESIZE' not in PAR:
            setattr(PAR, 'NODESIZE', 24)

        # how to invoke executables
        if 'MPIEXEC' not in PAR:
            setattr(PAR, 'MPIEXEC', 'srun')

        # optional additional SLURM arguments
        if 'SLURMARGS' not in PAR:
            setattr(PAR, 'SLURMARGS', '')

        # optional environment variable list VAR1=val1,VAR2=val2,...
        if 'ENVIRONS' not in PAR:
            setattr(PAR, 'ENVIRONS', '')

        # level of detail in output messages
        if 'VERBOSE' not in PAR:
            setattr(PAR, 'VERBOSE', 1)

        # where job was submitted
        if 'WORKDIR' not in PATH:
            setattr(PATH, 'WORKDIR', abspath('.'))

        # where output files are written
        if 'OUTPUT' not in PATH:
            setattr(PATH, 'OUTPUT', PATH.WORKDIR+'/'+'output')

        # where temporary files are written
        if 'SCRATCH' not in PATH:
            setattr(PATH, 'SCRATCH', join(os.getenv('CENTER1'), 'scratch', str(uuid4())))

        # where system files are written
        if 'SYSTEM' not in PATH:
            setattr(PATH, 'SYSTEM', PATH.SCRATCH+'/'+'system')

        # optional local scratch path
        if 'LOCAL' not in PATH:
            setattr(PATH, 'LOCAL', None)


    def submit(self, workflow):
        """ Submits workflow
        """
        # create scratch directories
        unix.mkdir(PATH.SCRATCH)
        unix.mkdir(PATH.SYSTEM)

        # create output directories
        unix.mkdir(PATH.OUTPUT)
        unix.mkdir(PATH.WORKDIR+'/'+'output.slurm')

        if not exists('./scratch'): 
            unix.ln(PATH.SCRATCH, PATH.WORKDIR+'/'+'scratch')

        workflow.checkpoint()

        # prepare sbatch arguments
        call('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--partition=%s ' % 't1small'
                + '--job-name=%s ' % PAR.TITLE
                + '--output %s ' % (PATH.WORKDIR+'/'+'output.log')
                + '--ntasks-per-node=%d ' % PAR.NODESIZE
                + '--nodes=%d ' % 1
                + '--time=%d ' % PAR.WALLTIME
                + findpath('seisflows.system') +'/'+ 'wrappers/submit '
                + PATH.OUTPUT)


    def run(self, classname, method, *args, **kwargs):

        self.checkpoint(PATH.OUTPUT, classname, method, args, kwargs)


        nodes_per_job = math.ceil(PAR.NPROC/float(PAR.NODESIZE))
        if nodes_per_job <= 2:
            partition = 't1small'
        else:
            partition = 't1standard'


        # submit job array
        stdout = check_output(
                   'sbatch %s ' % PAR.SLURMARGS
                   + '--job-name=%s ' % PAR.TITLE
                   + '--partition=%s ' % partition
                   + '--nodes=%d ' % math.ceil(PAR.NPROC/float(PAR.NODESIZE))
                   + '--ntasks-per-node=%d ' % PAR.NODESIZE
                   + '--ntasks=%d ' % PAR.NPROC
                   + '--time=%d ' % PAR.TASKTIME
                   + '--array=%d-%d ' % (0,(PAR.NTASK-1)%PAR.NTASKMAX)
                   + '--output %s ' % (PATH.WORKDIR+'/'+'output.slurm/'+'%A_%a')
                   + '%s ' % (findpath('seisflows.system') +'/'+ 'wrappers/run')
                   + '%s ' % PATH.OUTPUT
                   + '%s ' % classname
                   + '%s ' % method
                   + '%s ' % PAR.ENVIRONS,
                   shell=True)

        # keep track of job ids
        jobs = self.job_id_list(stdout, PAR.NTASK)

        # check job array completion status
        while True:
            # wait a few seconds between queries
            time.sleep(5)

            isdone, jobs = self.job_array_status(classname, method, jobs)
            if isdone:
                return


    def mpiexec(self):
        """ Specifies MPI exectuable; used to invoke solver
        """
        return 'mpirun -np %d ' % PAR.NPROC

