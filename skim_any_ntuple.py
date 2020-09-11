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

parser = argparse.ArgumentParser(description='Skim ntuples')
parser.add_argument('input', help='Input')
parser.add_argument('--debug', help='Increase me some verbosity dude', action='store_true')
parser.add_argument('--includeLHE', help='Include LHE weights', action='store_true')
parser.add_argument('--excludeSystematics', help='Exclude systematics', action='store_true')
parser.add_argument('--nominal', help='Only nominal trees', action='store_true')
parser.add_argument('--vjets', help='', action='store_true')
args = parser.parse_args()

print args

# set up the JobHandler
jh = JobHandler(work_dir="/project/etp3/eschanet/collect", name="skimCode", run_max=100)

# define the run script content
skim_script = """
#source ~/Code/hades/init.sh
#YTHONPATH=$PYTHONPATH:~/Code/hades/
#PATH=$PATH:~/ma/packages/hades/executables
#echo $PATH

pwd
cd ~/ma/packages/basic/
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
to_add = ["genWeight", "eventWeight", "leptonWeight", "jvtWeight", "bTagWeight", "pileupWeight", 'pileupWeightUp','pileupWeightDown','genWeightUp','genWeightDown',]
for b in to_add:
    branches.append(b)

vars = [
    "trigMatch_metTrig",
    "trigWeight_metTrig",
    "FS",
    "DatasetNumber",
    "RandomRunNumber",
    "RunNumber",
    "EventNumber",
    "GenHt",
    "met",
    "met_Phi",
    "mt",
    "nJet30",
    "nBJet30_MV2c10",
    "meffInc30",
    "LepAplanarity",
    "nLep_signal",
    "nLep_base",
    "lep1Pt",
    "AnalysisType",
]
selection =  "met>250 && nJet30>=2 && nLep_signal==1 && nLep_base==1" #strong 1L

for b in vars:
    branches.append(b)

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

lhe_weights = [
"LHE3Weight_MUR0.5_MUF0.5_PDF261000",
"LHE3Weight_MUR0.5_MUF1_PDF261000",
"LHE3Weight_MUR1_MUF0.5_PDF261000",
"LHE3Weight_MUR1_MUF1_PDF13000",
"LHE3Weight_MUR1_MUF1_PDF25300",
"LHE3Weight_MUR1_MUF1_PDF261000",
"LHE3Weight_MUR1_MUF1_PDF261001",
"LHE3Weight_MUR1_MUF1_PDF261002",
"LHE3Weight_MUR1_MUF1_PDF261003",
"LHE3Weight_MUR1_MUF1_PDF261004",
"LHE3Weight_MUR1_MUF1_PDF261005",
"LHE3Weight_MUR1_MUF1_PDF261006",
"LHE3Weight_MUR1_MUF1_PDF261007",
"LHE3Weight_MUR1_MUF1_PDF261008",
"LHE3Weight_MUR1_MUF1_PDF261009",
"LHE3Weight_MUR1_MUF1_PDF261010",
"LHE3Weight_MUR1_MUF1_PDF261011",
"LHE3Weight_MUR1_MUF1_PDF261012",
"LHE3Weight_MUR1_MUF1_PDF261013",
"LHE3Weight_MUR1_MUF1_PDF261014",
"LHE3Weight_MUR1_MUF1_PDF261015",
"LHE3Weight_MUR1_MUF1_PDF261016",
"LHE3Weight_MUR1_MUF1_PDF261017",
"LHE3Weight_MUR1_MUF1_PDF261018",
"LHE3Weight_MUR1_MUF1_PDF261019",
"LHE3Weight_MUR1_MUF1_PDF261020",
"LHE3Weight_MUR1_MUF1_PDF261021",
"LHE3Weight_MUR1_MUF1_PDF261022",
"LHE3Weight_MUR1_MUF1_PDF261023",
"LHE3Weight_MUR1_MUF1_PDF261024",
"LHE3Weight_MUR1_MUF1_PDF261025",
"LHE3Weight_MUR1_MUF1_PDF261026",
"LHE3Weight_MUR1_MUF1_PDF261027",
"LHE3Weight_MUR1_MUF1_PDF261028",
"LHE3Weight_MUR1_MUF1_PDF261029",
"LHE3Weight_MUR1_MUF1_PDF261030",
"LHE3Weight_MUR1_MUF1_PDF261031",
"LHE3Weight_MUR1_MUF1_PDF261032",
"LHE3Weight_MUR1_MUF1_PDF261033",
"LHE3Weight_MUR1_MUF1_PDF261034",
"LHE3Weight_MUR1_MUF1_PDF261035",
"LHE3Weight_MUR1_MUF1_PDF261036",
"LHE3Weight_MUR1_MUF1_PDF261037",
"LHE3Weight_MUR1_MUF1_PDF261038",
"LHE3Weight_MUR1_MUF1_PDF261039",
"LHE3Weight_MUR1_MUF1_PDF261040",
"LHE3Weight_MUR1_MUF1_PDF261041",
"LHE3Weight_MUR1_MUF1_PDF261042",
"LHE3Weight_MUR1_MUF1_PDF261043",
"LHE3Weight_MUR1_MUF1_PDF261044",
"LHE3Weight_MUR1_MUF1_PDF261045",
"LHE3Weight_MUR1_MUF1_PDF261046",
"LHE3Weight_MUR1_MUF1_PDF261047",
"LHE3Weight_MUR1_MUF1_PDF261048",
"LHE3Weight_MUR1_MUF1_PDF261049",
"LHE3Weight_MUR1_MUF1_PDF261050",
"LHE3Weight_MUR1_MUF1_PDF261051",
"LHE3Weight_MUR1_MUF1_PDF261052",
"LHE3Weight_MUR1_MUF1_PDF261053",
"LHE3Weight_MUR1_MUF1_PDF261054",
"LHE3Weight_MUR1_MUF1_PDF261055",
"LHE3Weight_MUR1_MUF1_PDF261056",
"LHE3Weight_MUR1_MUF1_PDF261057",
"LHE3Weight_MUR1_MUF1_PDF261058",
"LHE3Weight_MUR1_MUF1_PDF261059",
"LHE3Weight_MUR1_MUF1_PDF261060",
"LHE3Weight_MUR1_MUF1_PDF261061",
"LHE3Weight_MUR1_MUF1_PDF261062",
"LHE3Weight_MUR1_MUF1_PDF261063",
"LHE3Weight_MUR1_MUF1_PDF261064",
"LHE3Weight_MUR1_MUF1_PDF261065",
"LHE3Weight_MUR1_MUF1_PDF261066",
"LHE3Weight_MUR1_MUF1_PDF261067",
"LHE3Weight_MUR1_MUF1_PDF261068",
"LHE3Weight_MUR1_MUF1_PDF261069",
"LHE3Weight_MUR1_MUF1_PDF261070",
"LHE3Weight_MUR1_MUF1_PDF261071",
"LHE3Weight_MUR1_MUF1_PDF261072",
"LHE3Weight_MUR1_MUF1_PDF261073",
"LHE3Weight_MUR1_MUF1_PDF261074",
"LHE3Weight_MUR1_MUF1_PDF261075",
"LHE3Weight_MUR1_MUF1_PDF261076",
"LHE3Weight_MUR1_MUF1_PDF261077",
"LHE3Weight_MUR1_MUF1_PDF261078",
"LHE3Weight_MUR1_MUF1_PDF261079",
"LHE3Weight_MUR1_MUF1_PDF261080",
"LHE3Weight_MUR1_MUF1_PDF261081",
"LHE3Weight_MUR1_MUF1_PDF261082",
"LHE3Weight_MUR1_MUF1_PDF261083",
"LHE3Weight_MUR1_MUF1_PDF261084",
"LHE3Weight_MUR1_MUF1_PDF261085",
"LHE3Weight_MUR1_MUF1_PDF261086",
"LHE3Weight_MUR1_MUF1_PDF261087",
"LHE3Weight_MUR1_MUF1_PDF261088",
"LHE3Weight_MUR1_MUF1_PDF261089",
"LHE3Weight_MUR1_MUF1_PDF261090",
"LHE3Weight_MUR1_MUF1_PDF261091",
"LHE3Weight_MUR1_MUF1_PDF261092",
"LHE3Weight_MUR1_MUF1_PDF261093",
"LHE3Weight_MUR1_MUF1_PDF261094",
"LHE3Weight_MUR1_MUF1_PDF261095",
"LHE3Weight_MUR1_MUF1_PDF261096",
"LHE3Weight_MUR1_MUF1_PDF261097",
"LHE3Weight_MUR1_MUF1_PDF261098",
"LHE3Weight_MUR1_MUF1_PDF261099",
"LHE3Weight_MUR1_MUF1_PDF261100",
"LHE3Weight_MUR1_MUF1_PDF269000",
"LHE3Weight_MUR1_MUF1_PDF270000",
"LHE3Weight_MUR1_MUF2_PDF261000",
"LHE3Weight_MUR2_MUF1_PDF261000",
"LHE3Weight_MUR2_MUF2_PDF261000",
]

if not args.excludeSystematics:
    for sys in weight_sys:
        branches.append(sys)

output_path = "/project/etp4/eschanet/trees/v2-0/skims"

nominal='NoSys'

f = os.path.join(os.getcwd(),args.input)

name = os.path.splitext(os.path.basename(f))[0]

print("preparing skimming job(s) for {}".format(f))

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

#only for wjets, add lhe weights branches
if "wjets" in f and "NoSys" in f:
    if args.includeLHE:
        branchnames = branchnames + " ," + " ,".join(['"{}"'.format(b) for b in lhe_weights])

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
    ft = FinishedTrigger(outputfile)
    jh.add_job(name=name+"_merge", run_script=merge_script.format(outputfile=outputfile, inputfiles=" ".join(to_merge)), finished_func=ft, parent_tags=name+'_skim')
else:
    treenames = " ,".join(['"{}"'.format(t) for t in treenames])
    outputfile= os.path.join(output_path, os.path.basename(f))
    ft = FinishedTrigger(outputfile)
    jh.add_job(name=name, run_script=skim_script.format(inputfile=f, outputfile=outputfile, treenames=treenames, branches=branchnames, finished_func=ft, selection=selection))

jh.run_jobs()
