from flask import Flask,render_template
from flask_pymongo import PyMongo
import pandas as pd
import json

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/covid"
mongo = PyMongo(app)

@app.route("/")
def show_graph():
    # ob = {'Date':['2009-03-31','2009-03-30'],'High':[18.79,17.76]}
    # df = pd.DataFrame(data=ob)
    # chart_data = df.to_dict(orient='records')
    # chart_data = json.dumps(chart_data, indent=2)
    # data = {'chart_data': chart_data}
    twitter_sentiment = mongo.db.sentiment_predicted.find({})
    df =  pd.DataFrame(list(twitter_sentiment))
    df["prediction"] = df["result"].str[0]
    newdf = df.groupby(['prediction']).size().reset_index(name='counts')
    data = json.dumps(newdf.to_json(orient='records', lines=False))
    print(data)
    return render_template("index.html", data=data)


if __name__ == "__main__":
    app.run(debug=True)


'''
positive negative count group by 
'''