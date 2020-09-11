#!/usr/bin/env python3

import os
import argparse

from slurmy import JobHandler, Slurm, SingularityWrapper, SuccessTrigger

def clean_jobname(jobname):
    for c in [".", "-", "/"]:
        jobname = jobname.replace(c, "")
    return jobname

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def run():
    parser = argparse.ArgumentParser(description='Run stuff locally on etp')
    parser.add_argument('files', type=argparse.FileType('r'), nargs='+')
    parser.add_argument("-s",help="input sample",default=None)
    parser.add_argument("-selector",help="selector",default="OneLep")
    parser.add_argument("-writeTrees",help="sys or nominal",default="1")
    parser.add_argument("-deepConfig",help="input sample",default="SusySkim1LInclusive_Rel21.config")
    parser.add_argument("-outputPath",help="output path",default=None)
    parser.add_argument("-process",help="process tag to find your output",default=None)

    args = parser.parse_args()


    jobscript = """
    echo Running on host `hostname`
    echo Time is `date`
    echo Directory is `pwd`

    shopt -s expand_aliases

    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh

    pushd $testarea

    set -- ""
    acmSetup

    popd

    echo "run_xAODNtMaker"
    echo "    -s $sample"
    echo "    -selector $selector"
    echo "    -writeTrees $writeTrees"
    echo "    -deepConfig $deepConfig"
    echo "    -MaxEvents $maxEvents"
    echo "    -SkipEvents $skipEvents"

    run_xAODNtMaker -s $sample -selector $selector -writeTrees $writeTrees -deepConfig $deepConfig -MaxEvents $maxEvents -SkipEvents $skipEvents

    [[ "$?" = "0" ]] && mv ${outputPath}/submitDir/data-tree/  ${outputPath}/${groupset}/${mytag}/merged/${process}_${minEvent}_${maxEvent}_merged_processed_${writeTrees}.root
    """

    sw = SingularityWrapper('/cvmfs/atlas.cern.ch/repo/containers/images/singularity/x86_64-centos7.img')
    jh = JobHandler(wrapper = sw,work_dir="/project/etp2/eschanet/collect", name="run_ntuple")#, run_max=50)

    # sf = SuccessOutputFile()

    for exportopts, optDict in params:
        slurm = Slurm(export = exportopts)

        outputfile = os.path.abspath("{path}/{groupset}/{mytag}/merged/{process}_{minEvent}_{maxEvent}_merged_processed_{sys}.root.done".format(**optDict))

        jobname = "run_xAODNtMaker_{groupset}_{process}_{sys}_{minEvent}".format(**optDict)

        jobname = clean_jobname(jobname)

        print(jobname)
        ft = FinishedTrigger(outputfile)
        jh.add_job(backend = slurm, run_script = jobscript, output = outputfile, success_func = ft, name = jobname)

    jh.run_jobs()


if __name__ == "__main__":
    run()
