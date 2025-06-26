-- Section 1: Create Indexes on Frequently Queried Columns
-- Indexes can significantly speed up data retrieval operations by allowing the database
-- to quickly locate rows based on the values in the indexed columns.
-- This is particularly useful for columns used in WHERE clauses or JOIN conditions.

-- Create an index on the 'author' column in the 'BOOKS' table.
-- This will optimize queries that filter books by author.
CREATE INDEX idx_books_author ON BOOKS(author);

-- Create an index on the 'title' column in the 'BOOKS' table.
-- This will optimize queries that filter books by title.
CREATE INDEX idx_books_title ON BOOKS(title);

-- Create an index on the 'member_id' column in the 'TRANSACTIONS' table.
-- This will optimize queries that retrieve transactions for a specific member.
CREATE INDEX idx_transactions_member ON TRANSACTIONS(member_id);

-- Create an index on the 'book_id' column in the 'TRANSACTIONS' table.
-- This will optimize queries that retrieve transactions for a specific book.
CREATE INDEX idx_transactions_book ON TRANSACTIONS(book_id);


-- Section 2: Write a Query to Show Execution Plan for Finding Books by Author
-- The EXPLAIN (or EXPLAIN PLAN) statement allows you to see the execution plan
-- that the database query optimizer would use for a given SQL query.
-- This helps in understanding how the query will be processed and identifying
-- potential performance bottlenecks, especially whether an index is being used.

-- Display the execution plan for a query that selects all columns from the
-- 'BOOKS' table where the author is 'John Smith'.
-- The output of this command will detail the steps the database takes
-- to execute this particular SELECT statement, including if it uses the
-- 'idx_books_author' index we created earlier.
EXPLAIN SELECT * FROM BOOKS WHERE author = 'John Smith';
