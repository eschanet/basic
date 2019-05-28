#!/usr/bin/env python

import os, sys, argparse, glob

import ROOT

from slurmy import JobHandler, Slurm, FinishedTrigger

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def list_treenames(file):
    tf = ROOT.TFile.Open(file)
    trees_list = []
    for key in tf.GetListOfKeys():
        trees_list.append(key.GetName())
    tf.Close()
    return trees_list

# set up the JobHandler
jh = JobHandler(work_dir="/project/etp2/eschanet/collect", name="hadd", run_max=20)


merge_script = """
echo "running on $(hostname)"
echo ""

source /etc/profile.d/modules.sh
module load root
echo "using $(which root)"

hadd -f {outputfile} {inputfiles}

#echo "removing files that have been merged to save diskspace"

#rm  {inputfiles}
"""

tag = "1Lbb_v2-0-6"

output_path = "/project/etp2/eschanet/trees/v2-0/merged/bkg"
# processes = ["ttbar","singletop","wjets","zjets","diboson","multiboson","ttv","tth","vh","ttbar_allhad"]
# processes = ["singletop","wjets","zjets","diboson","multiboson","ttv","tth","vh","ttbar_allhad"]
processes = ["ttbar"]


systematics = [ "NoSys_noLHE"]

# systematics = [ "NoSys_noLHE",
#                 "EG_",
#                 "JET_BJES_",
#                 "JET_Comb_",
#                 "JET_EffectiveNP_Detector",
#                 "JET_EffectiveNP_Mixed",
#                 "JET_EffectiveNP_Modelling",
#                 "JET_EffectiveNP_Statistical",
#                 "JET_EtaIntercalibration_",
#                 "JET_Flavor_",
#                 "JET_JER_DataVsMC",
#                 "JET_JER_EffectiveNP",
#                 "JET_JvtEfficiency_",
#                 "JET_MassRes_",
#                 "JET_Pileup_",
#                 "JET_PunchThrough_",
#                 "JET_Rtrk_",
#                 "JET_SingleParticle_",
#                 "JET_RelativeNonClosure_",
#                 "MUON_",
#                 "MET_" ]

for process in processes:
    print "Starting with process {}".format(process)
    for sys in systematics:
        outputfile= os.path.join(output_path, "{}_merged_{}.root".format(process,sys))
        name = "{}_merged_{}".format(process,sys)

        to_merge = []
        for campaign in ["mc16e","mc16d","mc16a"]:
            # for f in glob.glob("/project/etp4/eschanet/ntuples/common/{}/{}/merged/{}_*tree_{}*done_noLHE.root".format(campaign,tag,process,sys)):
            for f in glob.glob("/project/etp4/eschanet/ntuples/common/{}/{}/merged/{}_*tree_{}*.root.done".format(campaign,tag,process,sys)):
                if process == "ttbar" and "ttbar_allhad" in f:
                    continue
                print f
                to_merge.append(f)
        ft = FinishedTrigger(outputfile)
        jh.add_job(name=name+"_hadd", run_script=merge_script.format(outputfile=outputfile,success_func=ft, inputfiles=" ".join(to_merge)))

jh.run_jobs()
