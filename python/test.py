import pickle

def main():
    with open("prediction.pickle", "rb") as handle:
        data = pickle.load(handle)
    print(data['call'])
    print(data['put'])

main()