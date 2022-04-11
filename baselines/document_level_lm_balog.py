import json
from util.text_analyzer import TextAnalyzer

CF_ALL_TERMS_EXPERT_LEVEL = 3443922
Count_of_all_answers = 33670
from elasticsearch import Elasticsearch
from util.elasticsearch import ElasticSearch as elasticsearch

es = Elasticsearch(urls="http://localhost", port="9200", timeout=600)
text_analyzer = TextAnalyzer()

Sampling = False
DOCUMENT_LEVEL_INDEX = "ef_legal_doc_level"
DOCUMENT_LEVEL_FIELD = "content"
queries_path = "../data/queries_bankruptcy.csv"
queries = open(queries_path, "r").read().splitlines()
if Sampling:
    queries = [queries[0]]


def find_top_1000_answers_for_query(query):
    es = elasticsearch()
    query = query.lower()
    bool_query = {
        "size": 1000,
        "query": {
            "bool": {
                "should": [{"match": {DOCUMENT_LEVEL_FIELD: query}}],
                "minimum_should_match": 0,
                "boost": 1.0,
            }
        },
    }
    answers = es.search(index=DOCUMENT_LEVEL_INDEX, body=bool_query)
    return answers["hits"]["hits"]


def find_an_doc_id_that_has_specific_query_term(term):
    print("find_an_doc_id_that_has_specific_query_term term:", term)
    es = elasticsearch()
    term = term.lower()
    bool_query = {
        "size": 1,
        "query": {
            "bool": {"must": [{"term": {DOCUMENT_LEVEL_FIELD: term}}], "boost": 1.0}
        },
    }
    candidates = es.search(index=DOCUMENT_LEVEL_INDEX, body=bool_query)
    return candidates["hits"]["hits"][0]["_id"]


def get_p_tc_doclevel(query_input_term):
    query_input_term = query_input_term.lower()
    doc_id_for_calculate_stats = find_an_doc_id_that_has_specific_query_term(
        query_input_term
    )
    body = {
        "fields": [DOCUMENT_LEVEL_FIELD],
        "offsets": True,
        "positions": True,
        "term_statistics": True,
        "field_statistics": True,
    }
    res = es.termvectors(
        index=DOCUMENT_LEVEL_INDEX, body=body, id=doc_id_for_calculate_stats
    )
    total_terms_fre_in_collection = CF_ALL_TERMS_EXPERT_LEVEL  # res["term_vectors"][EXPERT_LEVEL_FIELD]['field_statistics']['sum_ttf']
    query_term_freq_in_collection = res["term_vectors"][DOCUMENT_LEVEL_FIELD]["terms"][
        query_input_term
    ]["ttf"]
    p_tc_for_query_term = query_term_freq_in_collection / total_terms_fre_in_collection
    return p_tc_for_query_term


candidates_scores_doclevel = (
    {}
)  # structure: {"expert_id": {"query":[{answerIDXofExpert:score}, ..., {answeridN:score}]} }
docs_score_with_owner_candidate_id = (
    {}
)  # structure: {"query":[(doc_id, doc_score, expert_owner_id]}
beta_doc_level = CF_ALL_TERMS_EXPERT_LEVEL / Count_of_all_answers
p_tc_query_terms = {}
for query in queries:
    print("query: ", query)
    top_1000_answers_for_query = find_top_1000_answers_for_query(query)
    query_words = query.split(" ")
    for answer in top_1000_answers_for_query:
        owner_incremental_id = answer["_source"]["owner_incremental_id"]
        answer_id = answer["_id"]
        answer_text = answer["_source"][DOCUMENT_LEVEL_FIELD]
        answer_text_list = answer_text.split(" ")
        answer_len = len(answer_text_list)
        n_d = answer_len
        lambda_doc_level = beta_doc_level / (beta_doc_level + n_d)
        if query not in candidates_scores_doclevel:
            candidates_scores_doclevel[query] = {}
            docs_score_with_owner_candidate_id[query] = []
            candidates_scores_doclevel[query][
                owner_incremental_id
            ] = []  # list of tuple (answer_id, score)
        if (
            query in candidates_scores_doclevel
            and owner_incremental_id not in candidates_scores_doclevel[query]
        ):
            candidates_scores_doclevel[query][
                owner_incremental_id
            ] = []  # list of tuple (answer_id, score)
        total_score_for_this_query_term_to_this_doc = 1
        for query_term in query_words:
            if query_term in p_tc_query_terms:
                p_tc = p_tc_query_terms[query_term]
            else:
                p_tc = 0
                p_tc = get_p_tc_doclevel(query_term)
                p_tc_query_terms[query_term] = p_tc

            p_td = answer_text_list.count(query_term) / answer_len
            foreground_score = (1 - lambda_doc_level) * p_td
            background_score = lambda_doc_level * p_tc
            final_score_per_term = foreground_score + background_score
            final_score_per_term = final_score_per_term + 0.0000000001
            total_score_for_this_query_term_to_this_doc *= final_score_per_term
        candidates_scores_doclevel[query][owner_incremental_id].append(
            (answer_id, total_score_for_this_query_term_to_this_doc)
        )
        docs_score_with_owner_candidate_id[query].append(
            (
                answer_id,
                total_score_for_this_query_term_to_this_doc,
                owner_incremental_id,
            )
        )

model2_doclevel_ranking_path = "model_two_doclevel_ranking.dict"
with open(model2_doclevel_ranking_path, "w") as f:
    json.dump(candidates_scores_doclevel, f, indent=4)

model2_doclevel_scoreperdoc_ranking_path = (
    "model_two_doclevel_score_perdoc_ranking.dict"
)
with open(model2_doclevel_scoreperdoc_ranking_path, "w") as f:
    json.dump(docs_score_with_owner_candidate_id, f, indent=4)
