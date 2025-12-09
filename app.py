from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def index():
    number1 = 10
    number2 = 20
    sum_result = number1 + number2
    return render_template("template.html", number1=number1, number2=number2, sum_result=sum_result, image_url="image.png")

if __name__ == "__main__":
    app.run(debug=True)
