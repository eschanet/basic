#!/usr/bin/env python2

import os, sys, argparse, glob

import ROOT

from slurmy import JobHandler, Slurm

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def list_treenames(file):
    tf = ROOT.TFile.Open(file)
    trees_list = []
    for key in tf.GetListOfKeys():
        trees_list.append(key.GetName())
    # print trees_list
    tf.Close()
    return trees_list

skim_systs = True

# set up the JobHandler
jh = JobHandler(name="styx-skim", run_max=20)

# define the run script content
skim_script = """
#source ~/Code/hades/init.sh
#YTHONPATH=$PYTHONPATH:~/Code/hades/
#PATH=$PATH:~/ma/packages/hades/executables
#echo $PATH

pwd
cd ~/ma/packages/skimmer/
pwd
ls -1

source /etc/profile.d/modules.sh
module load root
which root

root -l -q 'NtupleSkimmer.C("{inputfile}", "{outputfile}", {{{treenames}}} , {{{branches}}}, "{selection}" )'
"""

merge_script = """
echo "running on $(hostname)"
echo ""

source /etc/profile.d/modules.sh
module load root
echo "using $(which root)"

hadd -f {outputfile} {inputfiles}

echo "removing files that have been merged to save diskspace"

rm  {inputfiles}
"""

#NtupleSkimmer.py --inputfile {inputfile} --outputfile {outputfile} --treenames {treenames} --branches {branches} --selection {selection}

# branches to write out
#######################
branches = []
# weights
to_add = ["genWeight", "eventWeight", "leptonWeight", "jvtWeight", "bTagWeight", "pileupWeight"]
for b in to_add:
    branches.append(b)
# general variables
branches.append("trigMatch_metTrig")
branches.append("trigWeight_metTrig")
branches.append("FS")
branches.append("DatasetNumber")
branches.append("RandomRunNumber")
branches.append("met")
branches.append("mt")
branches.append("mct2")
branches.append("mlb1")
branches.append("mbb")
branches.append("nJet30")
branches.append("nBJet30_MV2c10")

# lepton variables
branches.append("nLep_signal")
branches.append("nLep_base")


weight_sys = [
'leptonWeight_EL_EFF_ID_TOTAL_1NPCOR_PLUS_UNCOR__1down',
'leptonWeight_EL_EFF_ID_TOTAL_1NPCOR_PLUS_UNCOR__1up',
'leptonWeight_EL_EFF_Iso_TOTAL_1NPCOR_PLUS_UNCOR__1down',
'leptonWeight_EL_EFF_Iso_TOTAL_1NPCOR_PLUS_UNCOR__1up',
'leptonWeight_EL_EFF_Reco_TOTAL_1NPCOR_PLUS_UNCOR__1down',
'leptonWeight_EL_EFF_Reco_TOTAL_1NPCOR_PLUS_UNCOR__1up',
'leptonWeight_MUON_EFF_BADMUON_STAT__1down',
'leptonWeight_MUON_EFF_BADMUON_STAT__1up',
'leptonWeight_MUON_EFF_BADMUON_SYS__1down',
'leptonWeight_MUON_EFF_BADMUON_SYS__1up',
'leptonWeight_MUON_EFF_ISO_STAT__1down',
'leptonWeight_MUON_EFF_ISO_STAT__1up',
'leptonWeight_MUON_EFF_ISO_SYS__1down',
'leptonWeight_MUON_EFF_ISO_SYS__1up',
'leptonWeight_MUON_EFF_RECO_STAT__1down',
'leptonWeight_MUON_EFF_RECO_STAT__1up',
'leptonWeight_MUON_EFF_RECO_STAT_LOWPT__1down',
'leptonWeight_MUON_EFF_RECO_STAT_LOWPT__1up',
'leptonWeight_MUON_EFF_RECO_SYS__1down',
'leptonWeight_MUON_EFF_RECO_SYS__1up',
'leptonWeight_MUON_EFF_RECO_SYS_LOWPT__1down',
'leptonWeight_MUON_EFF_RECO_SYS_LOWPT__1up',
'leptonWeight_MUON_EFF_TTVA_STAT__1down',
'leptonWeight_MUON_EFF_TTVA_STAT__1up',
'leptonWeight_MUON_EFF_TTVA_SYS__1down',
'leptonWeight_MUON_EFF_TTVA_SYS__1up',
'bTagWeight_FT_EFF_B_systematics__1down',
'bTagWeight_FT_EFF_B_systematics__1up',
'bTagWeight_FT_EFF_C_systematics__1down',
'bTagWeight_FT_EFF_C_systematics__1up',
'bTagWeight_FT_EFF_Light_systematics__1down',
'bTagWeight_FT_EFF_Light_systematics__1up',
'bTagWeight_FT_EFF_extrapolation__1down',
'bTagWeight_FT_EFF_extrapolation__1up',
'bTagWeight_FT_EFF_extrapolation_from_charm__1down',
'bTagWeight_FT_EFF_extrapolation_from_charm__1up',
'jvtWeight_JET_JvtEfficiency__1down',
'jvtWeight_JET_JvtEfficiency__1up',
'pileupWeightUp',
'pileupWeightDown',
'genWeightUp',
'genWeightDown',
]

if skim_systs:
    for sys in weight_sys:
        branches.append(sys)

# define selection of skimming
##############################
# selection = met_trig + stau_veto + jpsi_veto + lep_author16_veto + " && met_Et > 100 && mll<60 && nJet20 > 0 && jetPt[0]>100"
selection =  "met>220 && nJet30<=3 && nJet30>=2 && nBJet30_MV2c10==2 && nLep_signal==1 && nLep_base==1"

# name of folder that contains the skimed files, e.g. "skim"
skim_folder = "skimmed"
output_path = "/project/etp3/eschanet/trees/v2-0/skimmed/"

# backgrounds
# for type in ["Signal", "BG", "Data"]:
for type in ["bkg", "data"]:
# for type in ["Signal"]:
    if type == "bkg":
        ntuple_path = "/project/etp3/eschanet/trees/v2-0/all/"
    elif type == "data":
        ntuple_path = "/project/etp4/eschanet/ntuples/common/full_run_2/v2-0/"

    for f in glob.glob(ntuple_path + "*.root"):
        # print f
        name = os.path.splitext(os.path.basename(f))[0]

        print("preparing skimming job(s) for {}".format(name))
        # replace characters that cannot be used in slurm job names
        for badsign in ".-/":
            name = name.replace(badsign, "")

        branchnames = " ,".join(['"{}"'.format(b) for b in branches])

        # retrieve list of trees, check if a tree with the same name is contained multiple times in the file
        treenames = []
        available_trees = sorted(list_treenames(f))
        for treename in available_trees:
            if available_trees.count(treename) == 1:
                treenames.append(treename)
            else:
                print("more than one tree named {} found in {} --> skipping ...".format(treename, f))
                continue
        del available_trees

        print("  -- found {} tree(s) to skim".format(len(treenames)))

        # if there are more than 50 trees, split into several jobs and merge later
        if len(treenames) > 50:
            i = 1
            to_merge = []
            for chunk in chunks(treenames, 25):
                print("creating chunk #{:02d}".format(i))
                trees = " ,".join(['"{}"'.format(t) for t in chunk])
                basename = "{:}_part{:03d}.root".format(os.path.splitext(os.path.basename(f))[0], i)
                outputfile= os.path.join(output_path, basename)
                jh.add_job(name=name+"_part{:03d}".format(i), run_script=skim_script.format(inputfile=f, outputfile=outputfile, treenames=trees, branches=branchnames, selection=selection), tags=name+"_skim")
                to_merge.append(outputfile)
                i += 1
            outputfile= os.path.join(output_path, os.path.basename(f))
            jh.add_job(name=name+"_merge", run_script=merge_script.format(outputfile=outputfile, inputfiles=" ".join(to_merge)), parent_tags=name+'_skim')
        else:
            treenames = " ,".join(['"{}"'.format(t) for t in treenames])
            outputfile= os.path.join(output_path, os.path.basename(f))
            jh.add_job(name=name, run_script=skim_script.format(inputfile=f, outputfile=outputfile, treenames=treenames, branches=branchnames, selection=selection))

jh.run_jobs()
