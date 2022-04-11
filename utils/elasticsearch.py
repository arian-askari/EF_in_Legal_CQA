import elasticsearch
from elasticsearch import helpers


class ElasticSearch:
    def __init__(self):
        # configure elasticsearch
        config = {"http://localhost": "9200"}
        self.es = elasticsearch.Elasticsearch(
            [
                config,
            ],
            timeout=300,
        )
        self.last_scroll_id = None

    def create_index(self, name, mapping, replace=False):
        if replace:
            self.delete_index(name)
        print("creating index, name: ", name)
        self.es.indices.create(index=name, body=mapping)
        print("index created successfully, index name: " + name)

    def delete_index(self, name):
        print("deleting index, name: ", name)
        self.es.indices.delete(index=name, ignore=[400, 404])
        print("index deleted successfully, index name: " + name)

    def index(self, documents, index_name, is_bulk=False):

        if is_bulk:
            try:
                # make the bulk call, and get a response
                response = helpers.bulk(
                    self.es, documents
                )  # chunk_size=1000, request_timeout=200
                print("\nRESPONSE:", response)
            except Exception as e:
                print("\nERROR:", e)

    def search(self, index, body):
        try:
            # make the bulk call, and get a response
            return self.es.search(index=index, body=body)
        except Exception as e:
            print("\nERROR:", e)

    def search_all_with_scorll(self, index, body):
        try:
            there_is_next_page = False

            resp = self.es.search(
                index=index,
                body=body,
                scroll="3m",  # time value for search
            )
            self.last_scroll_id = resp["_scroll_id"]
            if len(resp["hits"]["hits"]) >= 10000:
                there_is_next_page = True
            while there_is_next_page:
                resp_scroll = self.es.scroll(
                    scroll="3m",  # time value for search
                    scroll_id=self.last_scroll_id,
                )
                self.last_scroll_id = resp_scroll["_scroll_id"]
                resp["hits"]["hits"].extend(resp_scroll["hits"]["hits"])
                if len(resp_scroll["hits"]["hits"]) >= 10000:
                    there_is_next_page = True
                else:
                    there_is_next_page = False

            if there_is_next_page == False:
                self.last_scroll_id = None
                return resp
            # if hits is zero then there is no new!
            # return
        except Exception as e:
            print("\nERROR:", e)

    def get_with_id(self, index, id_):
        try:
            # make the bulk call, and get a response
            return self.es.get(index=index, id=id_)
        except Exception as e:
            print("\nERROR:", e)

    def termvectors(self, index, body, id):
        try:
            # make the bulk call, and get a response
            return self.es.termvectors(index=index, body=body, id=id)
        except Exception as e:
            print("\nERROR:", e)
