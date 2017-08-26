from similarity import Similarity
from flask import Flask, request
import spacy
import json
import sense2vec
from sense2vec.vectors import VectorMap
import settings
try:
    from urllib.parse import unquote
except ImportError:
    from urllib2 import unquote

app = Flask(__name__)

VECTORS_DIR = settings.VECTORS_DIR
v = VectorMap(128)
v.load(VECTORS_DIR)

sim = Similarity(
            spacy.load('en', parser=False, entity=False), v)


@app.route("/", methods=["GET"])
def show_similarity():
    if "query" in request.args:
        query = request.args["query"]
        similarity_results = sim(unquote(query))
        print(query)
        print(similarity_results)
        response = app.response_class(
            response=json.dumps(similarity_results),
            status=200,
            mimetype="application/json"
        )
        return response

    return "Please specify a query in GET parameter as query."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7000, debug=True)
