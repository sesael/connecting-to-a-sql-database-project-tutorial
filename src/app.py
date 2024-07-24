import os

os.system("pip install --upgrade pandas")
os.system("python -m pip install --upgrade 'sqlalchemy<2.0'")

from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True

from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Load the .env file variables
load_dotenv()
DB_USER = os.getenv('DB_USER', 'gitpod')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'sample-db')

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("Some environment variables are missing")

# Connect to the database using SQLAlchemy's create_engine function
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_string)

# Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
with engine.connect() as con:
    con.execute(text("""
    CREATE TABLE IF NOT EXISTS publishers(
        publisher_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY(publisher_id)
    );
    CREATE TABLE IF NOT EXISTS authors(
        author_id INT NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        middle_name VARCHAR(50) NULL,
        last_name VARCHAR(100) NULL,
        PRIMARY KEY(author_id)
    );
    CREATE TABLE IF NOT EXISTS books(
        book_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        total_pages INT NULL,
        rating DECIMAL(4, 2) NULL,
        isbn VARCHAR(13) NULL,
        published_date DATE,
        publisher_id INT NULL,
        PRIMARY KEY(book_id),
        CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
    );
    CREATE TABLE IF NOT EXISTS book_authors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY(book_id, author_id),
        CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
    );
    """))

    # Insert data into tables
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (1, 'O Reilly Media') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (2, 'A Book Apart') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (3, 'A K PETERS') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (4, 'Academic Press') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (5, 'Addison Wesley') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (6, 'Albert&Sweigart') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO publishers(publisher_id, name) VALUES (7, 'Alfred A. Knopf') ON CONFLICT DO NOTHING;"""))

    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (1, 'Merritt', NULL, 'Eric') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (2, 'Linda', NULL, 'Mui') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (3, 'Alecos', NULL, 'Papadatos') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (4, 'Anthony', NULL, 'Molinaro') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (5, 'David', NULL, 'Cronin') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (6, 'Richard', NULL, 'Blum') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (7, 'Yuval', 'Noah', 'Harari') ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (8, 'Paul', NULL, 'Albitz') ON CONFLICT DO NOTHING;"""))

    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (2, 'Facing the Intelligence Explosion', 91, 3.87, NULL, '2013-02-01', 7) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (6, 'Computing machinery and intelligence', 24, 4.17, NULL, '2009-03-22', 4) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7) ON CONFLICT DO NOTHING;"""))

    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (1, 1) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (2, 8) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (3, 7) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (4, 6) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (5, 5) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (6, 4) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (7, 3) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (8, 2) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (9, 4) ON CONFLICT DO NOTHING;"""))
    con.execute(text("""INSERT INTO book_authors (book_id, author_id) VALUES (10, 1) ON CONFLICT DO NOTHING;"""))

# Use pandas to print one of the tables as dataframes using read_sql function
with engine.connect() as conn:
    df = pd.read_sql(
        sql="SELECT * FROM authors;",
        con=conn.connection
    )

print(df.head())

