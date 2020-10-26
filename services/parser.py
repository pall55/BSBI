import pandas as pd
import re
import os


class Parser:
    """
    Objective: class to parse all files in specified location and generate (termID, documentID)
    Class Params:
        1)term_doc_df: DataFrame containing termID and documentID
        2)term_df: Dataframe contains terms with ids
    """

    def __init__(self):
        self.term_doc_df = pd.DataFrame(columns=["termID", "docID"])
        self.term_df = pd.DataFrame(columns=["terms"])

    def get_termID(self, terms):
        """
                Method updates class param 'term_df' if terms are not already present in the dataframe
        :param terms: list of unique terms in the file
        :return: Dataframe with index of the all input terms
        """
        new_terms = [term for term in terms if term not in self.term_df["terms"]]
        term_sub_df = pd.DataFrame(new_terms, columns=["terms"])
        term_df = self.term_df.append(term_sub_df, ignore_index=True)
        return list(term_df[term_df['terms'].isin(terms)].index)

    def update_term_doc_dictionary(self, term_ids, file_index):
        """
                Method updates class param 'term_doc_df'

        :param term_ids: term_id returned by get_termID
        :param file_index: index of current file in process
        :return: DateFrame with termID and docID
        """
        sub_df = pd.DataFrame(term_ids, columns=["termID"])
        sub_df["docID"] = file_index
        self.term_doc_df = self.term_doc_df.append(sub_df, ignore_index=True)

    def run(self, dir_path):
        """
                Method parses all txt files in the specified path
        :param dir_path: specifies the path where data resides
        :return: None
        """
        files = os.listdir(dir_path)
        for file_index in range(len(files)):
            with open(dir_path + "\\" + files[file_index], "r") as file_ref:
                words = set(re.split('[;:*\n?. ,]', file_ref.read()))
                term_ids = self.get_termID(words)
                self.update_term_doc_dictionary(term_ids, file_index)
