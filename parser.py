import json

class JsonParser:
    def __init__(self, path):
        self.path = path

    def pars(self):
        with open(self.path, "r", encoding="utf-8") as file:
            context = file.read()
        return context

if __name__ == "__main__":
    parser = JsonParser('data/videos.json')
    print(parser.pars())