import sqlalchemy as db
import pandas as pd


def main():
    engine = db.create_engine(f"mysql+pymysql://{'admin'}:{'azimi#1234'}@localhost/assignment")
    # connection = engine.connect()
    engine.connect()

    training_data = pd.read_csv('datasets/train.csv')
    training_data.to_sql('training_data3', engine, index=False)

    ideal_functions = pd.read_csv('datasets/ideal.csv')
    ideal_functions.to_sql('ideal_functions', engine, index=False)


if __name__ == '__main__':
    main()
