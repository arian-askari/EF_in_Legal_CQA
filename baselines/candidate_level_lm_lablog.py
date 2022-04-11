import sys
import json
CF_ALL_TERMS_EXPERT_LEVEL = 3443922
Count_of_all_answers = 33670
from elasticsearch import Elasticsearch
from utils.text_analyzer import TextAnalyzer
from utils.elasticsearch import ElasticSearch as elasticsearch
es = Elasticsearch(urls='http://localhost', port="9200", timeout=600)
text_analyzer = TextAnalyzer()

Sampling = False
CANDIDATE_LEVEL_FIELD = "ef_legal_user_leval"
CANDIDATE_LEVEL_INDEX = "content"
COUNT_OF_EXPERTS = 3741
queries_path = "../data/queries_bankruptcy.csv"
queries = open(queries_path, "r").read().splitlines()

if Sampling:
    queries = [queries[0]]

def get_all_exerpts():
    es = elasticsearch()
    bool_query = {
        "size": 10000,
        "query": {
           "match_all": {}
        },
    }
    candidates = es.search(index=CANDIDATE_LEVEL_INDEX, body=bool_query)
    return candidates['hits']['hits'][0]["_id"]

    es = elasticsearch()
    bool_query = {
        "size": 10000,

        "fields": [
            CANDIDATE_LEVEL_FIELD
        ]
        , "offsets": True,
        "positions": True,
        "term_statistics": True,
        "field_statistics": True
    }
    candidates = es.search(index=CANDIDATE_LEVEL_INDEX, body=bool_query)
    return candidates['hits']['hits']
def find_an_expert_id_that_has_specific_query_term(term):
    es = elasticsearch()
    term=term.lower()
    bool_query = {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {"term": {CANDIDATE_LEVEL_FIELD: term}}
                ],
                "boost": 1.0
            }
        }
    }
    candidates = es.search(index=CANDIDATE_LEVEL_INDEX, body=bool_query)
    return candidates['hits']['hits'][0]["_id"]
def get_p_tc(query_input_term):
    query_input_term = query_input_term.lower()
    expert_id_for_calculate_stats = find_an_expert_id_that_has_specific_query_term(query_input_term)
    body = {
        "fields": [
            CANDIDATE_LEVEL_FIELD
        ]
        , "offsets": True,
        "positions": True,
        "term_statistics": True,
        "field_statistics": True
    }
    res = es.termvectors(index=CANDIDATE_LEVEL_INDEX, body=body, id=expert_id_for_calculate_stats)
    total_terms_fre_in_collection = CF_ALL_TERMS_EXPERT_LEVEL#res["term_vectors"][EXPERT_LEVEL_FIELD]['field_statistics']['sum_ttf']
    query_term_freq_in_collection = res["term_vectors"][CANDIDATE_LEVEL_FIELD]["terms"][query_input_term]['ttf']
    p_tc_for_query_term = query_term_freq_in_collection/total_terms_fre_in_collection
    return p_tc_for_query_term
def get_lambda_expert(expert_id):
    body = {
        "fields": [
            CANDIDATE_LEVEL_FIELD
        ]
        , "offsets": True,
        "positions": True,
        "term_statistics": True,
        "field_statistics": True
    }
    expert = es.termvectors(index=CANDIDATE_LEVEL_INDEX, body=body, id=str(expert_id))

    doc_len = sum([v['term_freq'] for k, v in expert["term_vectors"][CANDIDATE_LEVEL_FIELD]["terms"].items()])
    n_expert = doc_len #count of terms written by this expert | doc_len of specific field for that expert !
    total_terms_fre_in_collection = expert["term_vectors"][CANDIDATE_LEVEL_FIELD]['field_statistics']['sum_ttf']
    beta =  total_terms_fre_in_collection #count of all terms in collection/count of experts
    lambda_expert_level =  beta/(n_expert+beta)
    return lambda_expert_level
def get_expert_score_per_term(query, expert_id):
    query_input = query.lower()
    query_term_p_tc = get_p_tc(query_input_term=query_input)
    try:
        lambda_expert_level =  get_lambda_expert(expert_id)
    except Exception as e:
        print("skipped, expert id: {}".format(expert_id))
        return "False"
    background_score = lambda_expert_level * query_term_p_tc
    p_td_for_all_answers = 0
    expert_answers = []

    res = es.get(index=CANDIDATE_LEVEL_INDEX, id=str(expert_id))
    expert_answers = res['_source']['all_answers_merged_in_array_content_whitespace_w_punc_lowercase']
    for answer in expert_answers:
        answer = answer['answer']
        answer_words = answer.split(" ")
        doc_len = len(answer)
        tf = answer_words.count(query_input)
        p_td = 0
        try:
            p_td = tf/doc_len
        except:
            pass
        p_td_for_all_answers+= p_td

    foreground_score = ((1-lambda_expert_level) * p_td_for_all_answers)
    expert_score_for_this_query_term = foreground_score + background_score
    return expert_score_for_this_query_term

candidates_scores_expertlevel = {} #structure: {"expert_id": {"query":[{answerIDXofExpert:score}, ..., {answeridN:score}]} }

skip_experts =  []
for query in queries:
    print("query: ", query)
    for expert_id in list(range(1,3742)):
        if expert_id in skip_experts:continue
        expert_total_score = 1

        query_words = query.split(" ")
        for query_term in query_words:
            ccc = get_expert_score_per_term(query_term, expert_id)
            if ccc == "False":
                skip_experts.append(expert_id)
                break
            expert_score_for_this_query_term =  ccc + 0.0000000001
            expert_total_score *= expert_score_for_this_query_term

        if expert_id in skip_experts:continue

        if query not in candidates_scores_expertlevel:
            candidates_scores_expertlevel[query] = []
        candidates_scores_expertlevel[query].append((expert_id, expert_total_score))

model2_expertlevel_ranking_path = "model_two_expertlevel_ranking.dict"
with open(model2_expertlevel_ranking_path, 'w') as f:
    json.dump(candidates_scores_expertlevel, f, indent=4)

