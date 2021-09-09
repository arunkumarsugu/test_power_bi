import pandas as pd


class Test:

    def __init__(self):
        self.df = None

    def power(self):
        data = {'name': ['nick', 'david', 'joe', 'ross'],
                'age': ['5', '10', '7', '6']}
        self.df = pd.DataFrame.from_dict(data)
        return self.df