
from configuration import dir_path
from services.parser import Parser
from services.inverter import Inverter

if __name__ == "__main__":
    parser = Parser()
    parser.run(dir_path)
    inverted_index = Inverter.invert_index(parser.term_doc_df)
    inverted_index = inverted_index.select("termID", "docID").toPandas()
    inverted_index['termID'] = inverted_index["termID"].apply(int)
    inverted_index.to_csv("invertedIndex.csv", index=False)
