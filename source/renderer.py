import json
from jinja2 import Environment, FileSystemLoader

# Choose your skin !
# Put in the skin variable the skin folder name (living in /skins)
skin = 'skin-2020'


with open("renderData.json","r") as f:
    renderData = json.load(f)

page = {
    'author': 'David Schwartz',
    'description': 'Dashboard for monitoring personal investments in cryptocurrencies.',
}

data = renderData['data']
fee = renderData['fee']
gain = renderData['gain']
eq_perf = renderData['eq_perf']
perf_topic = renderData['perf_topic']

# todo : round numerical values with 2 digits
# performance = round(performance, 2)     # round 2 digits after comma

file_loader = FileSystemLoader('skins')
env = Environment(loader=file_loader)

template = env.get_template(skin + '/index.html')

output = template.render(
    page=page,
    data=data,
    fee=fee,
    gain=gain,
    eq_perf=eq_perf,
    perf_topic=perf_topic)

f = open("output/index.html", "w")
f.write(output)
f.close()
