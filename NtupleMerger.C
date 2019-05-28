#include <vector>

void NtupleSkimmer(std::string inputfile, std::string outfile,
                   std::vector<std::string> treenames,
                   std::vector<std::string> branches,
                   std::string selection,
                   std::string mode="RECREATE"){

    TFile file_skimmed(outfile.c_str(), mode.c_str());
    file_skimmed.Close();

    for (auto treename : treenames){
      std::cout << "skimming tree " << treename << std::endl;

      // open files for each tree and close afterwards to overcome potential memory leaks
      TFile file_to_skim(inputfile.c_str(), "READ");
      TFile file_skimmed(outfile.c_str(), "UPDATE");

      TTree* tree_to_skim = (TTree*) file_to_skim.Get(treename.c_str());
      tree_to_skim->SetBranchStatus("*", 0);

      for (auto branch : branches){
        if (tree_to_skim->FindBranch(branch.c_str()))
            tree_to_skim->SetBranchStatus(branch.c_str(), 1);
        else
            std::cout << "NtupleSkimmer: no branch named " << branch << " in tree --> skipping ..." << std::endl;
      }
      file_skimmed.cd();
      TTree* tree_skimmed = tree_to_skim->CopyTree(selection.c_str());
      tree_skimmed->SetDirectory(&file_skimmed);
      tree_skimmed->Write(tree_skimmed->GetName(), TObject::kOverwrite);


      file_to_skim.Close();
      file_skimmed.Close();
    }


}