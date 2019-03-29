#!/usr/bin/env python2

import os, sys, argparse

import ROOT

parser = argparse.ArgumentParser(description='skim ntuples by reducing number of branches and/or applying selection')
parser.add_argument('--inputfile', help='input file to skim')
parser.add_argument('--outputfile', help='events will be written into this file')
parser.add_argument('--treenames', nargs='+', type=str, help='names of trees')
parser.add_argument('--branches', nargs='+', type=str, help='name of branches that will be written')
parser.add_argument('--selection', default="1", help='cutstring that will be applied')
args = parser.parse_args()

file_skimmed = ROOT.TFile(args.outputfile, "RECREATE")
file_skimmed.Close()
for treename in args.treenames:
    print("skimming tree {} ...".format(treename))

    file_to_skim = ROOT.TFile(args.inputfile, "READ")
    file_skimmed = ROOT.TFile(args.outputfile, "UPDATE")
    tree_to_skim = file_to_skim.Get(treename)

    tree_to_skim.SetBranchStatus("*", 0)
    for branchname in args.branches:
        if tree_to_skim.FindBranch(branchname):
            tree_to_skim.SetBranchStatus(branchname, 1)
        else:
            print("NtupleSkimmer: no branch named {} in tree --> skipping ...".format(branchname))

    file_skimmed.cd()
    tree_skimmed = tree_to_skim.CopyTree(args.selection)
    tree_skimmed.SetDirectory(file_skimmed)
    tree_skimmed.Write()

    file_to_skim.Close()
    file_skimmed.Close()
