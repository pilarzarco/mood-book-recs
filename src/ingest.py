import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi
from tqdm import tqdm

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

def upload_sample_data(limit=10000):
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client['BookProject']
    collection = db['books']
    batch = []

    print(f'Starting ingestion of {limit} records into MongoDB...')

    try: 
        # Load the sample data from the JSON file
        with open('data/goodreads_books.json', 'r') as f:
        # Insert the data into MongoDB
            for i, line in enumerate(tqdm(f, total=limit, desc="Processing Books")):
                if i >= limit:
                    break
                book_data = json.loads(line)
                
                cleaned_doc = {
                    "book_id": book_data.get("book_id"),
                    "title": book_data.get("title"),
                    "description": book_data.get("description"),
                    "average_rating": float(book_data.get("average_rating") or 0),
                    "num_pages":int(book_data.get("num_pages") or 0),
                    "ratings_count": int(book_data.get("ratings_count") or 0),
                    "publication_year": book_data.get("publication_year"),
                    "url": book_data.get("url"),
                    "image_url": book_data.get("image_url"),
                    "authors": [a.get("author_id") for a in book_data.get("authors", [])],
                    # This pulls the 'name' out of each shelf tag (e.g., 'biography')
                    # TODO: Keep in mind that this will include both genre tags and mood tags!
                    "tags": [tag.get("name") for tag in book_data.get("popular_shelves", [])]
                    
                }
                batch.append(cleaned_doc)
                if len(batch) == 500: 
                    collection.insert_many(batch)
                    batch = []
            # Upload any remaining books in the last batch
            if batch:
                collection.insert_many(batch)
            
            print("Data ingestion completed successfully.")

    except FileNotFoundError:
        print("Error: The file 'goodreads_books.json' was not found. Please ensure it is in the 'data' directory.")

# Separate funciton to upload authors data, since we want to create a separate collection for authors and link them by author_id

def upload_authors_data(limit=10000):
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client['BookProject']
    collection = db['authors']

    batch = []

    print(f'Starting ingestion of {limit} records into MongoDB...')

    with open('data/goodreads_book_authors.json', 'r') as f:
        for i, line in enumerate(tqdm(f, total=limit, desc="Processing Authors")):
            if i >= limit:
                break
            author_data = json.loads(line)

            cleaned_doc = {
                "author_id": author_data.get("author_id"),
                "name": author_data.get("name"),
                "average_rating": float(author_data.get("average_rating") or 0),
                "ratings_count": int(author_data.get("ratings_count") or 0),
                "text_reviews_count": int(author_data.get("text_reviews_count") or 0),
            }
            batch.append(cleaned_doc)

            if len(batch) == 500:
                collection.insert_many(batch)
                batch = []

        if batch:
            collection.insert_many(batch)

    # Create index on author_id for fast lookups
    collection.create_index("author_id")
    print(f"Authors ingestion complete. Index created on author_id.")

    
if __name__ == "__main__":
    #upload_sample_data()
    upload_authors_data()
