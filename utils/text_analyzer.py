import re

from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string

class TextAnalyzer:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.punctuations = list(string.punctuation)
        self.punctuations.append("''")

    def normalize(self, text, remove_html_tag=False):
        if text is None:
            return ""
        try:
            if remove_html_tag:
                return self.normalize(BeautifulSoup(text, "html5lib").text, False)
            return re.sub(' +', ' ', text.strip().replace("\n", " "))
        except:
            print("Exception: ", text)
            return ""

    def get_words(self, text, do_normalize=False, remove_punct=True):
        try:
            content = text
            if do_normalize:
                content = self.normalize(text, True)
            tokens = word_tokenize(content)
            if remove_punct==True:
                words = [word.lower() for word in tokens if word not in self.punctuations]
            else:
                words = [word.lower() for word in tokens]
            # words = [w for w in words if not w in self.stop_words]
            return words
        except Exception as e:
            print("error on tokenization: ", e, " | text: ", text)
            return []
