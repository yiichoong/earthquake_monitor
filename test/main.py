import pandas as pd

if __name__ == "__main__":
    df = pd.DataFrame({
        'name': ['Bob', 'John', 'Charlie'],
        'age': [24, 25, 27],
        'city': ['New York', 'London', 'Florida']
    })
    print(df)
