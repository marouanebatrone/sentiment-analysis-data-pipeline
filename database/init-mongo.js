db = db.getSiblingDB('news_db');

db.createCollection('raw_news');
db.createCollection('processed_news');

// Creating indexes for both raw_news and processed_news collections for better perform.

db.raw_news.createIndex({ "article.url": 1 }, { unique: true });
db.raw_news.createIndex({ "article.publishedAt": -1 });
db.raw_news.createIndex({ "source_api": 1 });
db.raw_news.createIndex({ "fetched_at": -1 });

db.processed_news.createIndex({ "article.url": 1 }, { unique: true });
db.processed_news.createIndex({ "processed_at": -1 });

print('Database and collections initialized successfully');