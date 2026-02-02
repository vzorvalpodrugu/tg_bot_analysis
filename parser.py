import json

class JsonParser:
    def __init__(self, path):
        self.path = path

    def pars(self):
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                context = json.load(file)
                context = list(context.items())[0][1] #list
                # print(type(context))
        except JsonParser:
            Exception("Json wrong")

        iterator = iter(context)
        while True:
            try:
                el = next(iterator)
                print(el)
            except StopIteration:
                break

        return context

if __name__ == "__main__":
    parser = JsonParser('data/videos.json')
    parser.pars()