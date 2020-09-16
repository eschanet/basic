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
jh = JobHandler(work_dir="/project/etp3/eschanet/collect", name="hadd", run_max=50)


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

tag = "prodApril-v2"

# output_path = "/project/etp4/eschanet/ntuples/preskims/prodApril-v2/bkg/"
output_path = "/project/etp2/eschanet/ntuples/preskims/prodApril-v2/bkg/"
# processes = ["ttbar","singletop","wjets","zjets","diboson","multiboson","ttv","tth","vh"]
# processes = ["singletop","wjets","zjets","diboson","multiboson","ttv","tth","vh","ttbar_allhad"]
# processes = ["zjets"]
processes = ["ttbar"]

# systematics = [
#                 "NoSys",
#                 "EG_",
#                 "JET_BJES_",
#                 "JET_EffectiveNP_Detector",
#                 "JET_EffectiveNP_Mixed",
#                 "JET_EffectiveNP_Modelling",
#                 "JET_EffectiveNP_Statistical",
#                 "JET_EtaIntercalibration_",
#                 "JET_Flavor_",
#                 # "JET_JER_DataVsMC",
#                 "JET_JER",
#                 # "JET_JvtEfficiency_",
#                 # "JET_MassRes_",
#                 "JET_Pileup_",
#                 "JET_PunchThrough_",
#                 # "JET_Rtrk_",
#                 "JET_SingleParticle_",
#                 # "JET_RelativeNonClosure_",
#                 "MUON_",
#                 "MET_",
#                  ]

systematics = [
    "EG_RESOLUTION_ALL",
    "EG_SCALE_AF2",
    "EG_SCALE_ALL",
    "JET_BJES_Response",
    "JET_EffectiveNP_Detector1",
    "JET_EffectiveNP_Detector2",
    "JET_EffectiveNP_Mixed1",
    "JET_EffectiveNP_Mixed2",
    "JET_EffectiveNP_Mixed3",
    "JET_EffectiveNP_Modelling1",
    "JET_EffectiveNP_Modelling2",
    "JET_EffectiveNP_Modelling3",
    "JET_EffectiveNP_Modelling4",
    "JET_EffectiveNP_Statistical1",
    "JET_EffectiveNP_Statistical2",
    "JET_EffectiveNP_Statistical3",
    "JET_EffectiveNP_Statistical4",
    "JET_EffectiveNP_Statistical5",
    "JET_EffectiveNP_Statistical6",
    "JET_EtaIntercalibration_Modelling",
    "JET_EtaIntercalibration_NonClosure_highE",
    "JET_EtaIntercalibration_NonClosure_negEta",
    "JET_EtaIntercalibration_NonClosure_posEta",
    "JET_EtaIntercalibration_TotalStat",
    "JET_Flavor_Composition",
    "JET_Flavor_Response",
    "JET_JER_EffectiveNP_1",
    "JET_JER_EffectiveNP_2",
    "JET_JER_EffectiveNP_3",
    "JET_JER_EffectiveNP_4",
    "JET_JER_EffectiveNP_5",
    "JET_JER_EffectiveNP_6",
    "JET_JER_EffectiveNP_7restTerm",
    "JET_Pileup_OffsetMu",
    "JET_Pileup_OffsetNPV",
    "JET_Pileup_PtTerm",
    "JET_Pileup_RhoTopology",
    "JET_PunchThrough_MC16",
    "JET_SingleParticle_HighPt",
    "MET_SoftTrk_ResoPara",
    "MET_SoftTrk_ResoPerp",
    "MET_SoftTrk_ScaleDown",
    "MET_SoftTrk_ScaleUp",
    "MUON_ID",
    "MUON_MS",
    "MUON_SAGITTA_RESBIAS",
    "MUON_SAGITTA_RHO",
    "MUON_SCALE",
    # "NoSys",
]

for process in processes:
    print "Starting with process {}".format(process)
    for sys in systematics:
        outputfile= os.path.join(output_path, "{}_merged_{}.root".format(process,sys))
        name = "{}_merged_{}".format(process,sys)

        to_merge = []
        for campaign in ["mc16e","mc16d","mc16a"]:
            # for f in glob.glob("/project/etp4/eschanet/ntuples/common/{}/{}/merged/{}_*tree_{}*done_noLHE.root".format(campaign,tag,process,sys)):
            for f in glob.glob("/project/etp4/eschanet/ntuples/common/{}/{}/merged/{}_*merged_processed_tree_{}*.root*".format(campaign,tag,process,sys)):
                if process == "ttbar" and "ttbar_allhad" in f:
                    continue
                print f
                to_merge.append(f)
        # if len(to_merge)%3!=0:
        #     print("{} probably incomplete!".format(process))
        print(len(to_merge))
        ft = FinishedTrigger(outputfile)
        jh.add_job(name=name+"_hadd", run_script=merge_script.format(outputfile=outputfile,finished_func=ft, inputfiles=" ".join(to_merge)))

jh.run_jobs()
