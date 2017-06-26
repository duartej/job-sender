#!/usr/bin/env python
""":mod:`workenvfactory` -- workenv abstract and concrete classes
=========================================================================

.. module:: workenvfactory
   :platform: Unix
   :synopsis: Module which contains the workenv abstract class and its
              concrete workenv classes.
              The workenv class defines the "work environment" of a job
              which is going to be sent to a cluster. It defines what
              kind of job is, what scripts needs and what requirements
              has. The base class defines the interface, which interacts
              with the client (a clusterspec instance). Each concrete 
              class needs to implement its own details. 
.. moduleauthor:: Jordi Duarte-Campderros <jorge.duarte.campderros@cern.ch>
"""

from abc import ABCMeta
from abc import abstractmethod

DEBUG=True
JOBEVT=500

class workenv(object):
    """ ..class:: workenv
    
    Class defining the "work environment" of a job which is 
    going to be sent to any cluster. It is a base class, which
    is a placeholder of the methods needed to build a concrete 
    implementation of it. 

    Virtual Methods:
     * __setneedenv__          
     * preparejobs
     * createbashscript     
     * sethowtocheckjobs 
     * sendjobs    
     * checkjobs 
     """
    __metaclass__ = ABCMeta

    def __init__(self,bashscriptname,**kw):
        """..class:: workenv()
        
        Abstract class to deal with a specific kind of job. 
        The following methods have to be implemented in the 
        concrete classes:
          * __setneedenv__
          * preparejobs
          * createbashscript     
          * sethowtocheckjobs 
          * sendjobs    
          * checkjobs 
        """
        # List of enviroment variables that should be defined in the shell
        # relevants to the job [ (environment_var,command_who_set_this_env), 
        # .. ]
        self.relevantvar = None
        # Name of the job type (Athena, ...) similar or probably the same 
        # as the base class
        self.typealias   = None
        # Name of the job
        self.jobname     = bashscriptname.split('.sh')[0]
        # Name of the script to be used to send jobs including the suffix 
        self.scriptname  = bashscriptname+'.sh'

        # set the relevant variables used to check the kind
        # of job is
        self.__setneedenv__()
        # Controling that the concrete method set the 'relevantvar'
        # datamember
        notfoundmess = "the __setneedenv__ class implemented in the class"
        notfoundmess+=" '%s' must inititalize the" % self.__class__.__name__
        notfoundmess+=" datamembers '%s'" 
        if not self.relevantvar:
            raise NotImplementedError(notfoundmess % ('relevantvar'))
        if not self.typealias:
            raise NotImplementedError(notfoundmess % ('typealias'))
        # Check if the enviroment is ok
        isenvset= self.checkenvironment()
        
        # Just error, send it and exit
        if type(isenvset) is tuple:
            message = "The environment is not ready for sending"
            message +=" an %s job. The environment" % self.typealias
            message += " variable '%s' is not set." % isenvset[2]
            message += " Do it with the '%s' command." % isenvset[1]
            raise RuntimeError(message)
           
    def checkenvironment(self):
        """..method:: checkenvironment() -> bool

        function to check if the environment is ready (the relevant 
        variables, libraries, etc..) are loaded in order to be able to 
        send this kind of jobs. 
        
        The function uses the 'relevantvar' datamember set in the 
        __setneedenv__ abstract method
        """
        import os
        for _var,_com in self.relevantvar:
            if not os.getenv(_var):
                return False,_com,_var
        return True
    
    @abstractmethod
    def checkfinishedjob(self,jobdsc):
        """..method:: checkfinishedjob(jobdsc) -> status
        
        using the datamember 'successjobcode' perform a check
        to the job (jobdsc) to see if it is found the expected
        outputs or success codes
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "checkfinishedjob()" % (self.__class__.__name__))
    
    @abstractmethod
    def __setneedenv__(self):
        """..method:: __setneedenv__() 

        method to set the datamember 'relevantvar' and the 'typealias'
        which depends of each job. The 'relevantvar' is a list of 
        tuples representing each step (if more than one) in the 
        setup process, where first element is the environment var 
        and the second element the command needed to export that var
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "__setneedenv__()" % (self.__class__.__name__))

    @abstractmethod
    def preparejobs(self,extrabash=''):
        """..method:: preparejobs()
        
        function to modify input scripts, make the cluster steer
        file (usually a bash script), build a folder hierarchy, etc.. in
        order to send a job. Depend on the type of job
        """
        raise NotImplementedError("Class %s doesn't implement "\
                "preparejobs()" % (self.__class__.__name__))
     
    @abstractmethod
    def createbashscript(self,**kw):
         """..method:: createbashscript 
         
         function which creates the specific bashscript(s). Depend on the 
         type of job
         """
         raise NotImplementedError("Class %s doesn't implement "\
                 "createbashscript()" % (self.__class__.__name__))

    @abstractmethod
    def sethowtocheckstatus(self):
         """...method:: sethowtocheckstatus()
         
         function to implement how to check if a job has
         succesfully finalized (looking at some key works in the output log
         and/or checking that the expected output files are there). Depend
         on the type of job
         """
         raise NotImplementedError("Class %s doesn't implement "\
                 "sethowtocheckstatus()" % (self.__class__.__name__))

## -- Concrete implementation: Blind job, the user provides everything
class blindjob(workenv):
    """Concrete implementation of a blind job. The user provides a bashscript
    name which corresponds with an actual bashscript in the working folder.
    The expected inputs are formed by:
        * 1 unic bashscript which contains inside %i pattern which is going
        to be substitute per the jobId
        * a list of files following the notation 'filename_i.suff'
        where 'i' stands for the jobID and 'suff' the suffix of the file
    
    Therefore the job splitting is based in the auxiliary files filename_i.suff
    """
    def __init__(self,bashscriptname,specificfile=None,**kw):
        """Concrete implementation of an blind job. 
        
        Parameters
        ----------
        nameofthejob: str
            generic name to this job and the name of an actual bashscript
            which must exist in the working dir 
        specificfile: str
            name of a file which should be used for each job, the file name should 
            contain a number which is associated to the job number: filename_i.suffix
                So the name to be passed should be filename.suffix
        """
        import glob
        from jobssender import getrealpaths,getremotepaths,getevt

        super(blindjob,self).__init__(bashscriptname,**kw)
        try:
            self.bashscript=getrealpaths(bashscriptname+'.sh')[0]
        except IndexError:
            raise RuntimeError('Bash script file not found %s' % bashscriptname)
        
        self.specificfiles = []
        if specificfile:
            self.specificfiles= sorted(glob.glob(specificfile.split('.')[0]+'_*'))
            if len(self.specificfiles) == 0:
                raise RuntimeError('Specific files not found %s' % specificfile)
        
        if kw.has_key('njobs'):
            self.njobs = int(kw['njobs'])
        else:
            self.njobs= 1

    def __setneedenv__(self):
        """..method:: __setneedenv__() 

        Relevant environment in an ATLAS job:
        """
        self.typealias = 'Blind'
        # just dummy
        self.relevantvar = [ ('PWD','echo') ] 

    def preparejobs(self,extra_asetup=''):
        """..method:: preparejobs() -> listofjobs
        
        main function which builds the folder structure
        and the needed files of the job, in order to be
        sent to the cluster
        A folder is created following the notation:
          * JOB_self.jobname_jobdsc.index
          
        """
        import os
        from jobssender import jobdescription

        cwd=os.getcwd()

        jdlist = []
        for i in xrange(self.njobs):
            # create a folder
            foldername = "%sJob_%s_%i" % (self.typealias,self.jobname,i)
            os.mkdir(foldername)
            os.chdir(foldername)
            # create the local bashscript
            self.createbashscript(i=i)
            # Registring the jobs in jobdescription class instances
            jdlist.append( 
                    jobdescription(path=foldername,script=self.jobname,index=i)
                    )
            jdlist[-1].state   = 'configured'
            jdlist[-1].status  = 'ok'
            jdlist[-1].workenv = self
            #self.setjobstate(jdlist[-1],'configuring') ---> Should I define one?
            os.chdir(cwd)

        return jdlist

    def createbashscript(self,**kw):
        """..method:: creatdbashscript()

        the bash script is copied in the local path      
        """
        import shutil
        import os
        import stat
        
        localcopy=os.path.join(os.getcwd(),os.path.basename(self.bashscript))
        if localcopy != self.bashscript:
            shutil.copyfile(self.bashscript,localcopy)
        # And re-point: WHY??
        #self.bashscript=localcopy

        with open(localcopy,"rw") as f:
            lines = f.readlines()
        f.close()
        newlines = map(lambda l: l.replace("%i",str(kw["i"])), lines)

        with open(localcopy,"w") as f1:
            f1.writelines(newlines)
        f1.close()
        # make it executable
        st = os.stat(localcopy)
        os.chmod(localcopy, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH )

    # DEPRECATED!!
    #def getlistofjobs(self):
    #    """..method:: getlistofjobs() -> [ listofjobs ]
    #    return the list of prepared jobs, if any, None otherwise

    #    :return: List of jobs prepared
    #    :rtype:  list(jobdescription)        
    #    """
    #    return self.joblist

    @staticmethod
    def checkfinishedjob(jobdsc,logfilename):
        """..method:: checkfinishedjob(jobdsc) -> status
        
        using the datamember 'successjobcode' perform a check
        to the job (jobdsc) to see if it is found the expected
        outputs or success codes
        """
        return 'fail' 

    def sethowtocheckstatus(self):
        """TO BE DEPRECATED!!!
        ..method:: sethowtocheckstatus()
        
        empty
        """
        self.succesjobcode=['','']

# Concrete workenv class for blind jobs
workenv.register(blindjob)

## -- Concrete implementation: Athena job
class athenajob(workenv):
    """Concrete implementation of an athena work environment. 
    An athena job could be called in two different modes:
      1. as jobOption-based [jO]
      2. as transformation job [tf]
    In both cases, an athena job should contain:
      * a bashscript name, which defines the cluster jobs, being
      bash script to deliver to the cluster
      * a list of input files
      * and depending of the mode (jO-based or tf):
        - [jO]: a jobOption python script feeding the athena exec
        - [tf]: a python script which contains in a single line (as
        it is) the string to be deliver to the Reco_tf.py exec. In that
        line, the input files related commands must be removed; but
        the output file related commands must be included (i.e. 
        --outputAODFile fileName.root) 
    
    See __init__ method to instatiate the class

    TO BE IMPLEMENTED: Any extra configuration variable to be used
    in the jobOption (via -c), can be introduced with the kw in the
    construction.
    """
    def __init__(self,bashscriptname,joboptionfile,
            inputfiles,athenamode = 'jo',**kw):
        """Instantiation

        Parameters
        ----------
        bashscriptname: str
            name of the bash script argument to the cluster sender command.
            Note that must be without suffix (.sh)
        joboptionfile: str
            path to the python script (jobOption or tf_parameters, depending
            the athena mode)
        inputfiles: str
            comma separated root inputfiles, wildcards are allowed
        athenamode: str, {'jo', 'tf'}
            athena mode, either jobOption-based or transformation job 
            (note default is jobOption mode)

        input_type: str, optional, { 'ESD', 'RAW', 'HITS', 'RDO', 'AOD' }
            only valid with athenamode=='tf'
        output_type: str, optional, 
            only valid with athenamode=='tf'
        evtmax: int, optional
            number of events to be processed
        njobs: int, optional
            number of jobs to be sent
        """
        from jobssender import getrealpaths,getremotepaths,getevt

        super(athenajob,self).__init__(bashscriptname,**kw)
        try:
            self.joboption=getrealpaths(joboptionfile)[0]
        except IndexError:
            raise RuntimeError('jobOption file not found %s' % joboptionfile)
        
        # check if it is a transformation job
        self.isTFJ = (athenamode == 'tf')
        self.useJOFile()
        if athenamode == 'tf':
            # To be extracted from the jobOption
            # ---- input file type
            i_if = self.tf_parameters.find("--input")
            if i_if == -1:
                raise RuntimeError('The jobOption must include the \'--inputTYPEFile\' option')
            self.tf_input_type = self.tf_parameters[i_if:].split()[0].replace("--input","").replace("File","")
            if self.tf_input_type not in [ 'ESD', 'RAW', 'HITS', 'RDO', 'AOD' ]:
                    raise AttributeError("Invalid input type found ='"+self.tf_input_type+"', "\
                            " see help(athenajob.__init__) to get the list of valid input types")
            # ---- output file and tpe
            i_of = self.tf_parameters.find("--output")
            if i_of == -1:
                raise RuntimeError('The jobOption must include the \'--outputTYPEFile\' option')
            self.tf_output_type = self.tf_parameters[i_of:].split()[0].replace("--output","").replace("File","")
            self.outputfile = self.tf_parameters[i_of:].split()[1]

            # --- remove the parameters related input file and outputfile
            removethis = []
            for _idnx in  [i_if, i_of ]:
                removethis.append( ' '.join(self.tf_parameters[_idnx:].split()[:2]) )

            # XXX: re-initialize tf_paramters
            for _rm in removethis:
                self.tf_parameters = self.tf_parameters.replace(_rm,"")
            #  The tf command per default, if no other was introduced by the user
            self.tf_command = "Reco_tf.py"
            if self.tf_parameters.find('_tf.py') != -1:
                thecommand = self.tf_parameters.split('_tf.py')[0].strip()
                self.tf_command = '{0}_tf.py'.format(thecommand)
            # -- remove it from parameters (commands/options)
            self.tf_parameters = self.tf_parameters.replace(self.tf_command,"")

        # Allowing EOS remote files
        if inputfiles.find('root://') == -1:
            self.remotefiles=False
            self.inputfiles=getrealpaths(inputfiles)
        else:
            self.remotefiles=True
            self.inputfiles,self.evtmax = getremotepaths(inputfiles)

        if len(self.inputfiles) == 0:
            raise RuntimeError('Not found the Athena inputfiles: %s' \
                    % inputfiles)
        
        if kw.has_key('evtmax') and int(kw['evtmax']) != -1:
            self.evtmax = int(kw['evtmax'])
        else:
            if not self.remotefiles:
                self.evtmax = getevt(self.inputfiles,treename='CollectionTree')

        if kw.has_key('njobs'):
            self.njobs = int(kw['njobs'])
        else:
            self.njobs= self.evtmax/JOBEVT

        # Get the evtperjob
        evtsperjob = self.evtmax/self.njobs
        # First event:0 last: n-1
        remainevts = (self.evtmax % self.njobs)-1
        
        self.skipandperform = []
        # Build a list of tuples containing the events to be skipped
        # followed by the number of events to be processed
        for i in xrange(self.njobs-1):
            self.skipandperform.append( (i*evtsperjob,evtsperjob) )
        # And the remaining
        self.skipandperform.append( ((self.njobs-1)*evtsperjob,remainevts) )

    def __setneedenv__(self):
        """..method:: __setneedenv__() 

        Relevant environment in an ATLAS job:
        """
        self.typealias = 'Athena'
        self.relevantvar =  [ ("AtlasSetup","setupATLAS"), ("CMTCONFIG","asetup") ] 

    def useJOFile(self):
        """..method:: useJOFile()

        the JobOption is copied in the local path if is a athena job or
        extracted the content if is a transformation job
        """
        if self.isTFJ:
            # if Reco_tf, just extract the content of the file
            with open(self.joboption) as f:
                _params = f.readline()
                self.tf_parameters = _params.replace("\n","")
                f.close()
            return
        # Otherwise, copy it in the local path
        import shutil
        import os
        
        localcopy=os.path.join(os.getcwd(),os.path.basename(self.joboption))
        if localcopy != self.joboption:
            shutil.copyfile(self.joboption,localcopy)
        # And re-point
        self.joboption=localcopy
    
    def jobOption_modification(self):
        """Be sure that the FilesInput and SkipEvents are used
        accordingly in a jo-based
        """
        lines = []
        with open(self.joboption) as f:
            lines = f.readlines()
        # secure and lock the FilesINput and skipEvents provided
        # by the user
        prelines = [ "athenaCommonFlags.FilesInput.set_Value_and_Lock(FilesInput)\n" ,\
                "athenaCommonFlags.SkipEvents.set_Value_and_Lock(SkipEvents)\n",
                "athenaCommonFlags.EvtMax.set_Value_and_Lock(EvtMax)\n"]
        try:
            impline = filter(lambda (i,l): 
                    l.find("from AthenaCommon.AthenaCommonFlags import athenaCommonFlags") == 0, enumerate(lines))[0][0]
        except IndexError:
            print "\033[1;m31WARNING\0331;m Weird jobOption, which does not content the athenaCommonFlags."\
                    " The input files, and skip events options are ignored, be sure that your jobOption"\
                    " contains the proper input files for the job"
            return

        for k in xrange(len(prelines)):
            lines.insert(impline+(k+1),prelines[k])
        # Post-lines: be sure the Event max and skip events are properly used
        lines +="\ntheApp.EvtMax=athenaCommonFlags.EvtMax()\n"
        lines +="svcMgr.EventSelector.SkipEvents=athenaCommonFlags.SkipEvents()\n"

        with open(self.joboption,"w") as f:
            f.writelines(lines)


    def preparejobs(self,extra_asetup=''):
        """..method:: preparejobs() -> listofjobs
        
        main function which builds the folder structure
        and the needed files of the job, in order to be
        sent to the cluster
        """
        import os
        # Obtaining some Athena related-info (asetup,release,...)
        usersetupfolder = self.getuserasetupfolder()
        athenaversion = os.getenv('AtlasVersion')
        compiler      = self.getcompiler()
        
        # if is an athena job, be sure that the FilesInput and SkipEvents
        # orders are received and locked, i.e. modify accordingly the JO
        if not self.isTFJ:
            self.jobOption_modification()

        # setting up the folder structure to send the jobs
        # including the bashscripts
        return self.settingfolders(usersetupfolder,athenaversion,compiler,extra_asetup)


    def getuserasetupfolder(self):
        """..method:: getuserasetupfolder() -> fullnameuser
        get the asetup folder (where the asetup was launch)
        """
        import os

        ldfolders = os.getenv('LD_LIBRARY_PATH')
        user = os.getenv('USER')
        basedir = None
        for i in ldfolders.split(':'):
            if i.find(user) != -1 and i.find('InstallArea'):
                basedir=i[:i.find('InstallArea')-1]
        # check that the folder exists
        if not basedir or not os.path.isdir(basedir):
            message = 'Check the method (getuserasetupfolder) to'
            message += 'extract the user base directory. The algorithm'
            message += 'didn\'t find the path'
            raise RuntimeError(message)

        return basedir

    def getcompiler(self):
        """..method:: getcompiler() -> compilername
        
        Get the compiler version X.Y.Z, returning gccXY
        """
        import platform
        return platform.python_compiler().lower().replace(' ','').replace('.','')[:-1]

    def createbashscript(self,**kw):
        """..method:: createbashscript 
         
        function which creates the specific bashscript(s). Depend on the 
        type of job
        """
        import os
        import datetime,time

        class placeholder(object):
            def __init__(self):
                self.setupfolder=None
                self.version=None
                self.gcc =None
                self.extra_asetup=''

            def haveallvars(self):
                if not self.setupfolder or not self.version or not self.gcc:
                    return False
                return True
        
        ph = placeholder()
        for var,value in kw.iteritems():
            setattr(ph,var,value)
        
        if not ph.haveallvars():
            message = "Note that the asetup folder, the Athena version and"
            message += " the version of the gcc compiler are needed to build the"
            message += " bashscript"
            raise RuntimeError(message)

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        bashfile = '#!/bin/bash\n\n'
        bashfile += '# File created by the %s class [%s]\n\n' % (self.__class__.__name__,timestamp)
        bashfile += 'cd '+ph.setupfolder+'\n'
        bashfile += 'source $AtlasSetup/scripts/asetup.sh %s,%s,here %s\n' % (ph.version,ph.gcc,ph.extra_asetup)
        bashfile += 'cd -\n'
        # Create a guard against malformed Workers (those which uses the same $HOME)
        bashfile += 'tmpdir=`mktemp -d`\ncd $tmpdir;\n\n'
        if self.isTFJ:
            # Transformation job
            # XXX: Create a separate file containing the list of input files:
            fileslist_name = "fileslist_{0}.txt".format(self.scriptname.replace(".sh",""))
            with open(fileslist_name,"w") as _f:
                _f.write(' '.join(self.inputfiles)+' ')
                _f.close()
            # convert the list of files into a space separated string (' '.join(self.inputfiles)
            bashfile += '{0} --fileValidation False --maxEvents {1}'\
                    ' --skipEvents {2} --ignoreErrors \'True\' {3} --input{4}File {5} '\
                    '--output{6}File {7}'.format(self.tf_command,ph.nevents,ph.skipevts,self.tf_parameters,
                    self.tf_input_type,'`cat '+os.path.join(os.getcwd(),fileslist_name)+'`',self.tf_output_type,self.outputfile)
                    #self.tf_input_type,' '.join(self.inputfiles),self.tf_output_type,self.outputfile)
        else:
            # athena.py jobOption.py job
            bashfile += 'cp %s .\n' % self.joboption
            bashfile +='athena.py -c "SkipEvents=%i; EvtMax=%i; FilesInput=%s;" ' % \
                    (ph.skipevts,ph.nevents,str(self.inputfiles))
            # Introduce a new key with any thing you want to introduce in -c : kw['Name']='value'
            bashfile += self.joboption+" \n"
        bashfile +="\ncp *.root %s/\n" % os.getcwd()
        # remove the tmpdir
        bashfile +="rm -rf $tmpdir\n"
        f=open(self.scriptname,"w")
        f.write(bashfile)
        f.close()
        os.chmod(self.scriptname,0755)

    def replace_str_infile(self,strtosubst,finalstr,filename=None):
        """Replaces a string inside a file, useful for per-job dependent
        string

        Parameters
        ----------
        strtosubst: str
            the string to be substituted
        finalstr: the expression to substitute
        filename: str [Default: self.scriptname]
            the name of the file
        """
        if not filename:
            filename=self.scriptname
        with open(filename) as _f:
            oldfiledata = _f.read()
        filedata=oldfiledata.replace(strtosubst,str(finalstr))
        # return to write
        with open(filename,"w") as _fo:
            _fo.write(filedata)
            _fo.close()

    def settingfolders(self,usersetupfolder,athenaversion,gcc,extra_asetup=''):
        """..method:: settingfolders()
        create the folder structure to launch the jobs: for each job
        a folder is created following the notation:
          * AthenaJob_self.jobname_jobdsc.index
          
        """
        import os
        from jobssender import jobdescription

        cwd=os.getcwd()

        jdlist = []
        i=0
        for (skipevts,nevents) in self.skipandperform:
            # create a folder
            foldername = "%sJob_%s_%i" % (self.typealias,self.jobname,i)
            os.mkdir(foldername)
            os.chdir(foldername)
            # create the local bashscript
            self.createbashscript(setupfolder=usersetupfolder,\
                    version=athenaversion,\
                    gcc=gcc,skipevts=skipevts,nevents=nevents,extra_asetup=extra_asetup)
            # XXX: Provisional (or not): Some keywords to be substitute 
            # (job-index dependent)
            self.replace_str_infile("%JOBNUMBER_PLUS_ONE",i+1)
            # Registring the jobs in jobdescription class instances
            jdlist.append( 
                    jobdescription(path=foldername,script=self.jobname,index=i)
                    )
            jdlist[-1].state   = 'configured'
            jdlist[-1].status  = 'ok'
            jdlist[-1].workenv = self
            #self.setjobstate(jdlist[-1],'configuring') ---> Should I define one?
            os.chdir(cwd)
            i+=1

        return jdlist

    # DEPRECATED!!
    #def getlistofjobs(self):
    #    """..method:: getlistofjobs() -> [ listofjobs ]
    #    return the list of prepared jobs, if any, None otherwise

    #    :return: List of jobs prepared
    #    :rtype:  list(jobdescription)        
    #    """
    #    return self.joblist

    @staticmethod
    def checkfinishedjob(jobdsc,logfilename):
        """..method:: checkfinishedjob(jobdsc) -> status
        
        using the datamember 'successjobcode' perform a check
        to the job (jobdsc) to see if it is found the expected
        outputs or success codes

        FIXME: Static class do not have make use of the self?
        """
        #if self.isTFJ:
        #    succesjobcode=['PyJobTransforms.main','trf exit code 0']
        #else:
        #    succesjobcode=['Py:Athena','INFO leaving with code 0: "successful run"']
        succesjobcode_tf=['PyJobTransforms.main','trf exit code 0']
        succesjobcode_jo=['Py:Athena','INFO leaving with code 0: "successful run"']
        import os
        # Athena jobs outputs inside folder defined as:
        #folderout = os.path.join(jobdsc.path,'LSFJOB_'+str(jobdsc.ID))
        # outfile
        #logout = os.path.join(folderout,"STDOUT")
        # -- defined as logfilename
        logout = os.path.join(jobdsc.path,logfilename)
        if not os.path.isfile(logout):
            if DEBUG:
                print "Not found the logout file '%s'" % logout
            return 'fail'

        f = open(logout)
        lines = f.readlines()
        f.close()
        # usually is in the end of the file
        for i in reversed(lines):
            if i.find(succesjobcode_jo[-1]) != -1:
                return 'ok'
            if i.find(succesjobcode_tf[-1]) != -1:
                return 'ok'
        return 'fail' 

    def sethowtocheckstatus(self):
        """TO BE DEPRECATED!!!
        ..method:: sethowtocheckstatus()
        
        An Athena job has succesfully finished if there is the line
        'Py:Athena            INFO leaving with code 0: "successful run"'
        """
        self.succesjobcode=['Py:Athena','INFO leaving with code 0: "successful run"']



# Concrete workenv class for athena jobs
workenv.register(athenajob)


## -- Concrete implementation: EUtelescope/Marlin job
class marlinjob(workenv):
    """Concrete implementation of an ILC-Marlin work environment. 
    An Marlin job should contain:
      * a bashscript name, which defines the cluster jobs and is the
      bash script to deliver to the cluster
      * a list of input files
      * a list of steering files in order of processing
    
    See __init__ method to instatiate the class
    """
    def __init__(self,bashscriptname,steeringfile,
            inputfiles,**kw):
        """Instantiation

        Parameters
        ----------
        bashscriptname: str
            name of the bash script argument to the cluster sender command.
            Note that must be without suffix (.sh)
        steeringfilelist: str
            path to the steering files 
        inputfiles: str
            comma separated root inputfiles, wildcards are allowed

        evtmax: int, optional
            number of events to be processed
        njobs: int, optional
            number of jobs to be sent
        gear_file: str, optional
            the gear file to use
        """
        from jobssender import getrealpaths,getremotepaths
        from jobssender import getevt_alibava as getevt

        super(marlinjob,self).__init__(bashscriptname,**kw)
        
        try:
            self.steering_file = getrealpaths(steeringfile)[0]
        except IndexError:
            raise RuntimeError('Steering file not found {0}'.format(steeringfile))
        
        # Allowing EOS remote files
        if inputfiles.find('root://') == -1:
            self.remotefiles=False
            self.inputfiles=getrealpaths(inputfiles)
        else:
            self.remotefiles=True
            self.inputfiles,self.evtmax = getremotepaths(inputfiles)

        if len(self.inputfiles) == 0:
            raise RuntimeError('Not found the inputfiles: {0}'.format(inputfiles))
        
        if kw.has_key('evtmax') and int(kw['evtmax']) != -1:
            self.evtmax = int(kw['evtmax'])
        else:
            if not self.remotefiles:
                self.evtmax = getevt(self.inputfiles)

        if kw.has_key('njobs'):
            self.njobs = int(kw['njobs'])
        else:
            self.njobs= self.evtmax/JOBEVT

        if kw.has_key('gear_file'):
            self.gear_file = getrealpaths(kw['gear_file'])[0]
        else:
            self.gear_file = getrealpaths('gear.xml')[0]

        if kw.has_key('is_alibava_conversion'):
            self.is_alibava_conversion = bool(kw['is_alibava_conversion'])
        else:
            self.is_alibava_convesion = False

        # if alibava conversion allow only one job
        if self.is_alibava_conversion:
            self.njobs = 1
        
        # Get the evtperjob
        evtsperjob = self.evtmax/self.njobs
        # First event:0 last: n-1
        remainevts = (self.evtmax % self.njobs)-1
        
        self.skipandperform = []
        # Build a list of tuples containing the events to be skipped
        # followed by the number of events to be processed
        for i in xrange(self.njobs-1):
            self.skipandperform.append( (i*evtsperjob,evtsperjob) )
        # And the remaining
        self.skipandperform.append( ((self.njobs-1)*evtsperjob,remainevts) )

    def __setneedenv__(self):
        """Relevant environment in an Marlin job: MARLIN
        """
        self.typealias = 'Marlin'
        self.relevantvar =  [ ("MARLIN","source") ] 

    def use_steering_file(self):
        """The steering file is copied in the local path
        """
        import shutil
        import os
        
        localcopy=os.path.join(os.getcwd(),os.path.basename(self.joboption))
        if localcopy != self.steering_file:
            shutil.copyfile(self.steering_file,localcopy)
        # And re-point
        self.steering_file=localcopy

    def _set_field_at(self,key_list,the_field,the_value,text_wanted=False):
        """Helper function (could be deattached from the class)
        to update a field in a xmltodict dictionary, parsed
        from a Marlin xml steering file.

        Parameters
        ----------
        key_list: list(xmltodict.OrderedDict)
            the list of dictionaries built from a `parameters` keys
        the_field: str
            the required field to be modified (defined with `@name`)
        the_value: any type
            the value (converted to str) which is set to the `@value`
            key
        
        """
        from xmltodict_jb import xmltodict 
        
        # -- Number of events to process
        try:
            _index = filter(lambda (i,x): x['@name'] == the_field,enumerate(key_list))[0][0]
        except IndexError:
            # It is not in here, it must be created
            key_list.append(xmltodict.OrderedDict( [('@name',the_field)] ))  
            _index = -1
        # Using only the @value field, therefore, remove the #text if any
        try:
            key_list[_index].pop('#text')
        except KeyError:
            pass
        if text_wanted:
            # Using only the #text field, remove the value if any
            try:
                key_list[_index].pop('@value')
            except KeyError:
                pass
            key_list[_index]['#text'] = u'{0}'.format(the_value)
        else:
            key_list[_index]['@value'] = u'{0}'.format(the_value)


    def _get_active_processor(self,thexmldict,processor_type,processor_name=None):
        """Given a processor type (and optionaly a processor name), found and
        return the associated dictionary to that processor in order to manipulate
        its options

        Parameters
        ----------
        thexmldict: collections.OrderedDict
            the output of the xmltodict.parser over a marlin xml steering file
        processor_type: str
            the processor name (C++ class name) 
        processor_name: str, optional
            the instance name of the processor

        Return
        ------
        theprocdict: collections.OrderedDict
            a (deep) copy to the dictionary containing the processor
        """
        theprocdict = None
        # Get the processor from the processor branch
        i = 0
        for pr in  filter(lambda x: x[u'@type'] == processor_type,\
                thexmldict['marlin']['processor']):
            if processor_name == "" or processor_name == None:
                # take the first one and get the instance name from it
                theprocdict=pr
                processor_name = theprocdict[u'@name']
                break
            elif pr[u'@name'] == processor_name:
                # found it
                theprocdict = pr
                break
            ++i
        # Error codes
        if not theprocdict:
            if i == 0:
                raise RuntimeError('Processor "{0}" not found in the '\
                        'template steering file (or invalid Marlin'\
                        ' steering file)'.format(processor_type))
            elif i != 0 and processor_name != "":
                if theprocdict[u'@name'] != processor_name:
                    raise RuntimeError('Processor name "{0}" does not corresponds to'\
                        ' any processor in the steering file'.format(processor_name))
        # Check that it is activated 
        if len(filter(lambda x: x['@name'] == processor_name,\
                thexmldict['marlin']['execute']['processor'])) == 0:
            raise RuntimeError('Processor "{0}" not activated!'.format(processor_name))
        
        return theprocdict

    def steering_file_modification(self):
        """Substitute the steering files with the concrete values
        
        Note
        ----
        if is_alibava_conversion is activated, then specific actions
        must be taken into account, as just send 1 job, and re-interpret 
        the input file as the file for the converter
        """
        from xmltodict_jb import xmltodict 
        import shutil

        with open(self.steering_file) as f:
            xml_steering = xmltodict.parse(f.read())
        # The global parameters: Just be sure they are in the steering
        # file in order to be updated with the command line afterwards
        par_list = xml_steering['marlin']['global']['parameter']
        # -- Number of events to process
        self._set_field_at(par_list,u'MaxRecordNumber',self.evtmax)
        # -- Skip events -- 
        self._set_field_at(par_list,u'SkipNEvents',0)
        # -- Gear files
        self._set_field_at(par_list,u'GearXMLFile',self.gear_file)
        # -- input files
        inputfiles_str = ''
        for _f in self.inputfiles:
            inputfiles_str += _f+" "
        self._set_field_at(par_list,u'LCIOInputFiles',inputfiles_str[:-1],text_wanted=True)

        # Some particularities for the ALIBAVA raw converter jobs
        if self.is_alibava_conversion:
            # -- remove the global inputFiles
            self._set_field_at(par_list,u'LCIOInputFiles',"",text_wanted=True)
            # -- the inputfile is sended to the AlibavaConverter processor
            converter = self._get_active_processor(xml_steering,"AlibavaConverter")
            par_list_converter = converter['parameter']
            self._set_field_at(par_list_converter,u'InputFileName',inputfiles_str[:-1],text_wanted=True)
            # -- Remove the MaxRecordNumber
            self._set_field_at(par_list,u'MaxRecordNumber',"",text_wanted=True)
        
        
        # a backup copy?
        shutil.copyfile(self.steering_file,self.steering_file[::-1].replace('.','.kcb_',1)[::-1])
        with open(self.steering_file,"w") as f:
            xmltodict.unparse(xml_steering,output=f,pretty=True)


    def preparejobs(self,asetup_extra=""):
        """Main function which builds the folder structure
        and the needed files of the job, in order to be
        sent to the cluster
        A folder is created following the notation:
          * JOB_self.jobname_jobdsc.index

        Note
        ----
        if is_alibava_conversion is activated, then specific actions
        must be taken into account, as just send 1 job, and re-interpret 
        the input file as the file for the converter
        """
        import os
        from jobssender import jobdescription

        cwd=os.getcwd()
        # Modify a few
        # Create the copy of the steering file
        self.steering_file_modification()

        jdlist = []
        for (i,(skipevts,nevents)) in enumerate(self.skipandperform):
            # create a folder
            foldername = "{0}Job_{1}_{2}".format(self.typealias,self.jobname,i)
            os.mkdir(foldername)
            os.chdir(foldername)
            # create the local bashscript
            self.createbashscript(skipevents=skipevts,nevents=nevents)
            # Registring the jobs in jobdescription class instances
            jdlist.append( 
                    jobdescription(path=foldername,script=self.jobname,index=i)
                    )
            jdlist[-1].state   = 'configured'
            jdlist[-1].status  = 'ok'
            jdlist[-1].workenv = self
            #self.setjobstate(jdlist[-1],'configuring') ---> Should I define one?
            os.chdir(cwd)
        return jdlist

    def createbashscript(self,**kw):
        """Creates the specific bashscript(s). Depend on the 
        type of job
        """
        import os
        import datetime,time

        class placeholder(object):
            def __init__(this):
                this.skipevents=None
                this.nevents=None

            def haveallvars(this):
                if this.skipevents is None or this.nevents is None:
                    return False
                return True
        
        ph = placeholder()
        for var,value in kw.iteritems():
            setattr(ph,var,value)
        
        if not ph.haveallvars():
            print ph.skipevents is None,(ph.nevents is None)
            message = "Several variables needed were not setup"
            raise RuntimeError(message)

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        bashfile = '#!/bin/bash\n\n'
        bashfile += '# File created by the %s class [%s]\n\n' % (self.__class__.__name__,timestamp)
        bashfile += '# ASSUMING job propagated environment variables. NO SETUP \n'
        # Create a guard against malformed Workers (those which uses the same $HOME)
        bashfile += 'tmpdir=`mktemp -d`\ncd $tmpdir;\n\n'
        # Marlin job
        bashfile += 'cp {0} .\n'.format(self.steering_file)
        inputfiles_str = ''
        for _f in self.inputfiles:
            inputfiles_str += _f+" "
        # Not including some of the options when dealing with 
        # alibava conversion jobs
        if self.is_alibava_conversion:
            maxrecordnumber_cmmd=""
            skipevents_cmmd     = ""
            inputfiles_cmmd      = ""
        else:
            maxrecordnumber_cmmd= "--global.MaxRecordNumber={0}".format(ph.nevents)
            skipevents_cmmd     = "--global.SkipNEvents={0}".format(ph.skipevents)
            inputfiles_cmmd     = "--global.LCIOInputFiles=\"{0}\"".format(inputfiles_str[:-1])

        bashfile +='Marlin --global.GearXMLFile={2} {0} {1} {3} {4}\n'.format(maxrecordnumber_cmmd,\
                skipevents_cmmd,self.gear_file,inputfiles_cmmd,self.steering_file)
        bashfile +="\ncp *.root *.slcio {0}/\n".format(os.getcwd())
        # remove the tmpdir
        bashfile +="rm -rf $tmpdir\n"
        f=open(self.scriptname,"w")
        f.write(bashfile)
        f.close()
        os.chmod(self.scriptname,0755)

    @staticmethod
    def checkfinishedjob(jobdsc,logfilename):
        """Using the datamember 'successjobcode' perform a check
        to the job (jobdsc) to see if it is found the expected
        outputs or success codes

        FIXME: TO BE UPDATEd
        """
        #succesjobcode=''
        #import os
        ## -- defined as logfilename
        #logout = os.path.join(jobdsc.path,logfilename)
        #if not os.path.isfile(logout):
        #    if DEBUG:
        #        print "Not found the logout file '{0}'".format(logout)
        #    return 'fail'

        #f = open(logout)
        #lines = f.readlines()
        #f.close()
        ## usually is in the end of the file
        #for i in reversed(lines):
        #    if i.find(succesjobcode) != -1:
        #        return 'ok'
        #return 'fail' 
        return 'ok'

    def sethowtocheckstatus(self):
        """TO BE DEPRECATED!!!
        
        NOT YET DEFINED 
        """
        self.succesjobcode=['']

# Concrete workenv class for athena jobs
workenv.register(marlinjob)
