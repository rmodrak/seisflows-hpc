
import sys

from getpass import getuser
from os.path import abspath, exists
from uuid import uuid4
from seisflows.tools import unix
from seisflows.config import ParameterError, custom_import

PAR = sys.modules['seisflows_parameters']
PATH = sys.modules['seisflows_paths']


class tigergpu_sm(custom_import('system', 'slurm_sm')):
    """ Specially designed system interface for tigergpu.princeton.edu

      See parent class for more information.
    """

    def check(self):
        """ Checks parameters and paths
        """
        if 'MPIEXEC' in PAR:
            print 'Ignoring user-supplied MPIEXEC parameter'

        # where job was submitted
        if 'WORKDIR' not in PATH:
            setattr(PATH, 'WORKDIR', abspath('.'))

        # where temporary files are written
        if 'SCRATCH' not in PATH:
            setattr(PATH, 'SCRATCH', PATH.WORKDIR+'/'+'scratch')

        super(tiger_lg, self).check()


    def run(self, classname, funcname, hosts='all', **kwargs):
        """  Runs tasks in serial or parallel on specified hosts
        """
        self.checkpoint()
        self.save_kwargs(classname, funcname, kwargs)

        if hosts == 'all':
            # run on all available nodes
            call('srun '
                    + '--wait=0 '
                    + join(findpath('seisflows-hpc'), 'system/wrapper/dsh_tigergpu')
                    + PATH.OUTPUT + ' '
                    + classname + ' '
                    + funcname + ' '
                    + PAR.ENVIRONS)

        elif hosts == 'head':
            # run on head node
            call('srun '
                    + '--wait=0 '
                    + '--ntasks=1 '
                    + '--nodes=1 '
                    + join(findpath('seisflows'), 'system/wrappers/run')
                    + PATH.OUTPUT + ' '
                    + classname + ' '
                    + funcname + ' '
                    + PAR.ENVIRONS)


    def mpiexec(self):
        """ Specifies MPI executable used to invoke solver
        """
        return 'mpirun -np %d --mca plm isolated --mca ras simulator ' % PAR.NPROC


