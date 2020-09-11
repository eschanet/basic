#!/usr/bin/env python

import ROOT

import os,sys,glob
import argparse
import re
import pprint

from machete import pythonHelpers as ph

from slurmy import JobHandler, Slurm

jh = JobHandler(work_dir="/project/etp/eschanet/collect", name="campaignHadder", run_max=100)

merge_script = """
echo "running on $(hostname)"
echo ""

source /etc/profile.d/modules.sh
module load root
echo "using $(which root)"

hadd -f {outputfile} {inputfiles}

echo "removing files that have been merged to save diskspace"

#rm  {inputfiles}
"""

parser = argparse.ArgumentParser(description='Hadd files across MC campaigns')
parser.add_argument('input', help='Text file with samples to hadd.', nargs='+')
# parser.add_argument('-naming-scheme', help='Naming scheme of signal samples. Use regex.', default='C1N2_Wh_hbb_\d*p\d_\d*p\d')
# parser.add_argument('-naming-scheme', help='Naming scheme of signal samples. Use regex.', default='leptoquark_[azA-Z0-9]*_[azA-Z0-9]*_[azA-Z0-9]*_M\d*')
# parser.add_argument('-naming-scheme', help='Naming scheme of signal samples. Use regex.', default='[a-zA-Z0-9]*_LQ[a-zA-Z]{1}_[a-zA-Z]*_[a-zA-Z]*_\dp\d_[a-zA-Z]*_\dp\d_[a-zA-Z]*_\dp\d_M\d{3,4}')
parser.add_argument('-naming-scheme', help='Naming scheme of signal samples. Use regex.', default='[A-Za-z]{2}_onestepCC_\d*_\d*_\d*')
parser.add_argument('-production', help='Production tag.', default='v2-0-signal_fix')
# parser.add_argument('-output-file', help='Name of hadded output tree.', default='allTrees_v1_20_signal_wh.root')
#parser.add_argument('-output-file', help='Name of hadded output tree.', default='allTrees_v1_20_signal_onestep.root')
parser.add_argument('--skip-hadd', help='Skip the actual hadding. Useful for testing purposes', action='store_true')
parser.add_argument('--ignore-incomplete', help='Hadd even if one campaign is missing', action='store_true')
args = parser.parse_args()

pattern = re.compile(args.naming_scheme)

base_path = "/project/etp2/eschanet/SUSY1L_EventSelection_21.2.60/SUSY1L_EventSelection/source/SusySkim1LInclusive/data/samples/mc16e/"
full_paths = [os.path.join(base_path, path) for path in args.input]

output_path = "/project/etp4/eschanet/ntuples/preskims/v2-0-signal_fix/signal/"

for f in full_paths:
    with open(f,'r') as i:
        for line in i:
            if ph.is_comment(line):
                continue
            match = re.search(pattern,line)
            if match:
                point = match.group()

                print point
                #TODO: make nicer
                if "onestep" in point.lower():
                    #onestep points have different formatting at tree- and AOD-level
                    point = point.replace("onestepCC", "oneStep")
                elif "LQ" in point:
                    point = "leptoquark_" + point.replace('ld_0p3_beta_0p5_hnd_1p0_','') + "_"
                searcher = "{}*merged_processed*.root".format(point)
                print(searcher)
                files_mc16a = glob.glob("/project/etp4/eschanet/ntuples/common/mc16a/{}/merged/{}".format(args.production,searcher))
                files_mc16d = glob.glob("/project/etp4/eschanet/ntuples/common/mc16d/{}/merged/{}".format(args.production,searcher))
                files_mc16e = glob.glob("/project/etp4/eschanet/ntuples/common/mc16e/{}/merged/{}".format(args.production,searcher))

                if len(files_mc16a)==0 or len(files_mc16d)==0 or len(files_mc16e)==0:
                    print "mc16a: %i"%len(files_mc16a)
                    print "mc16d: %i"%len(files_mc16d)
                    print "mc16e: %i"%len(files_mc16e)
                    print "WARNING  -  {} is not complete".format(point)

                    if not args.ignore_incomplete:
                        continue
                    print "WARNING  -  Ignoring incomplete!".format(point)

                inputfiles = " ".join(sorted(files_mc16a)) + " " + " ".join(sorted(files_mc16d)) + " " + " ".join(sorted(files_mc16e))
                #pprint.pprint(inputfiles)
                outputfile = output_path + "{}_merged_processed.root".format(point)
                jh.add_job(name=point+"_campaignHadder", run_script=merge_script.format(outputfile=outputfile, inputfiles=inputfiles), output=outputfile)

jh.run_jobs()
