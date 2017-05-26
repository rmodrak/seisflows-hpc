
import os
import math
import sys
import time

from os.path import abspath, basename, join
from seisflows.tools import msg
from seisflows.tools import unix
from seisflows.tools.tools import call, findpath, saveobj, timestamp
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class slurm_ft(custom_import('system', 'slurm_lg')):
    """ An interface through which to submit workflows, run tasks in serial or 
      parallel, and perform other system functions.

      By hiding environment details behind a python interface layer, these 
      classes provide a consistent command set across different computing
      environments.

      Intermediate files are written to a global scratch path PATH.SCRATCH,
      which must be accessible to all compute nodes.

      Optionally, users can provide a local scratch path PATH.LOCAL if each
      compute node has its own local filesystem.

      For important additional information, please see 
      http://seisflows.readthedocs.org/en/latest/manual/manual.html#system-configuration
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
            raise ParameterError(PAR, 'NTASK')

        # number of cores per task
        if 'NPROC' not in PAR:
            raise ParameterError(PAR, 'NPROC')

        # number of cores per node
        if 'NODESIZE' not in PAR:
            raise ParameterError(PAR, 'NODESIZE')

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
            setattr(PATH, 'SCRATCH', PATH.WORKDIR+'/'+'scratch')

        # where system files are written
        if 'SYSTEM' not in PATH:
            setattr(PATH, 'SYSTEM', PATH.SCRATCH+'/'+'system')

        # optional local scratch path
        if 'LOCAL' not in PATH:
            setattr(PATH, 'LOCAL', None)


    ### job array methods

    def resubmit_failed_job(self, classname, funcname, jobs, taskid):
        with open(PATH.SYSTEM+'/'+'job_id', 'w') as file:
            call(self.resubmit_cmd(classname, funcname, taskid),
                stdout=file)

        with open(PATH.SYSTEM+'/'+'job_id', 'r') as file:
            line = file.readline()
            jobid = line.split()[-1].strip()

        # remove failed job from list
        jobs.pop(taskid)

        # add resubmitted job to list
        jobs.insert(taskid, jobid)

        return jobs


    def resubmit_cmd(self, classname, funcname, taskid):
        return ('sbatch '
                + '%s ' % PAR.SLURMARGS
                + '--job-name=%s ' % PAR.TITLE
                + '--nodes=%d ' % math.ceil(PAR.NPROC/float(PAR.NODESIZE))
                + '--ntasks-per-node=%d ' % PAR.NODESIZE
                + '--ntasks=%d ' % PAR.NPROC
                + '--time=%d ' % PAR.TASKTIME
                + '--output=%s ' % (PATH.WORKDIR+'/'+'output.slurm/'+'%j')
                + '--export=TASKID=%d ' % taskid
                + findpath('seisflows.system') +'/'+ 'wrappers/run '
                + PATH.OUTPUT + ' '
                + classname + ' '
                + funcname + ' ' 
                + PAR.ENVIRONS)


    def taskid(self):
        """ Gets number of running task
        """
        if os.getenv('SLURM_ARRAY_TASK_ID'):
            return int(os.getenv('SLURM_ARRAY_TASK_ID'))
        else:
            try:
                return int(os.getenv('TASKID'))
            except:
                raise Exception("TASKID environment variable not defined.")


    def job_array_status(self, classname, funcname, jobs):
        """ Determines completion status of one or more jobs
        """
        states = []
        for taskid, job in enumerate(jobs):
            state = self._query(job)
            if state in ['TIMEOUT']:
                print msg.TimoutError % (classname, funcname, job, PAR.TASKTIME)
                sys.exit(-1)
            elif state in ['FAILED', 'NODE_FAIL']:
                print ' task %d failed, retrying' % taskid
                jobs = self.resubmit_failed_job(classname, funcname, jobs, taskid)
                states += [0]

            elif state in ['COMPLETED']:
                states += [1]
            else:
                states += [0]

        isdone = all(states)

        return isdone, jobs


