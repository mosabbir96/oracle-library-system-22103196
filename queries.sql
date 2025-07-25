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


-- Section 3: Display Member Borrowing Trends with LAG function
-- This query uses the LAG window function to show the 'issue_date' of the
-- previous transaction for each member, ordered by issue date.
-- This helps in analyzing borrowing trends and patterns for individual members.
SELECT
    member_id,
    transaction_id,
    issue_date,
    LAG(issue_date) OVER (PARTITION BY member_id ORDER BY issue_date) AS previous_transaction_date
FROM TRANSACTIONS
ORDER BY member_id, issue_date;


-- Section 4: Stored Procedure for Issuing Books (ISSUE_BOOK)
-- This stored procedure handles the logic for issuing a book to a member.
-- It performs checks for book availability, generates a new transaction ID,
-- records the transaction, and updates the book's available copies.
-- It includes transaction management (START TRANSACTION, COMMIT, ROLLBACK)
-- and error handling.
DELIMITER $$

CREATE PROCEDURE ISSUE_BOOK (
    IN p_member_id INT,
    IN p_book_id INT
)
proc_label: BEGIN
    DECLARE v_available INT DEFAULT 0;
    DECLARE v_max_transaction_id INT DEFAULT 0;

    -- Define an exit handler for SQL exceptions to rollback the transaction
    -- and return an error message.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SELECT 'Error: Could not issue book.' AS message;
    END;

    -- Start a transaction to ensure atomicity of operations.
    START TRANSACTION;

    -- Check book availability and lock the row to prevent race conditions.
    SELECT available_copies INTO v_available
    FROM BOOKS
    WHERE book_id = p_book_id
    FOR UPDATE;

    -- If the book does not exist, rollback and return an error.
    IF v_available IS NULL THEN
        ROLLBACK;
        SELECT 'Error: Book does not exist.' AS message;
        LEAVE proc_label; -- Exit the procedure
    END IF;

    -- If the book is not available (copies <= 0), rollback and return an error.
    IF v_available <= 0 THEN
        ROLLBACK;
        SELECT 'Error: Book not available for issuing.' AS message;
        LEAVE proc_label; -- Exit the procedure
    END IF;

    -- Get the maximum transaction_id to generate a new unique ID.
    SELECT IFNULL(MAX(transaction_id), 0) INTO v_max_transaction_id FROM TRANSACTIONS;

    -- Insert a new record into the TRANSACTIONS table.
    INSERT INTO TRANSACTIONS (
        transaction_id, member_id, book_id, issue_date, due_date, return_date, fine_amount, status
    ) VALUES (
        v_max_transaction_id + 1,
        p_member_id,
        p_book_id,
        CURDATE(), -- Current date for issue_date
        DATE_ADD(CURDATE(), INTERVAL 14 DAY), -- Due date 14 days from issue_date
        NULL,      -- return_date is initially NULL
        0.00,      -- No fine initially
        'Pending'  -- Transaction status
    );

    -- Decrease the available copies of the book.
    UPDATE BOOKS
    SET available_copies = available_copies - 1
    WHERE book_id = p_book_id;

    -- Commit the transaction if all operations are successful.
    COMMIT;

    -- Return a success message with the new transaction ID.
    SELECT CONCAT('Book issued successfully. Transaction ID: ', v_max_transaction_id + 1) AS message;

END proc_label $$

DELIMITER ;


-- Section 5: Function to Calculate Fine (CALCULATE_FINE)
-- This function calculates the fine for a given transaction based on overdue days.
-- It charges ₹5 per day for books returned after their due date or for books
-- currently overdue.
DELIMITER $$

CREATE FUNCTION CALCULATE_FINE(p_transaction_id INT)
RETURNS DECIMAL(6,2)
DETERMINISTIC
BEGIN
    DECLARE v_due_date DATE;
    DECLARE v_return_date DATE;
    DECLARE v_today DATE;
    DECLARE v_overdue_days INT DEFAULT 0;
    DECLARE v_fine DECIMAL(6,2) DEFAULT 0.00;

    SET v_today = CURDATE();

    -- Retrieve the due_date and return_date for the specified transaction.
    SELECT due_date, return_date
    INTO v_due_date, v_return_date
    FROM TRANSACTIONS
    WHERE transaction_id = p_transaction_id;

    -- Calculate overdue days:
    -- If the book has been returned, calculate days between return_date and due_date.
    -- Otherwise, calculate days between today's date and due_date.
    IF v_return_date IS NOT NULL THEN
        SET v_overdue_days = DATEDIFF(v_return_date, v_due_date);
    ELSE
        SET v_overdue_days = DATEDIFF(v_today, v_due_date);
    END IF;

    -- If there are overdue days, calculate the fine (₹5 per day).
    IF v_overdue_days > 0 THEN
        SET v_fine = v_overdue_days * 5;
    ELSE
        SET v_fine = 0.00;
    END IF;

    RETURN v_fine;
END $$

DELIMITER ;

-- Example of how to call the CALCULATE_FINE function
SELECT CALCULATE_FINE(1) AS fine_for_transaction_1;


-- Section 6: Trigger for Updating Available Copies After Book Return
-- This trigger automatically increments the 'available_copies' in the 'BOOKS' table
-- when a transaction's status changes from anything other than 'Returned' to 'Returned'.
DELIMITER $$

CREATE TRIGGER update_available_copies_after_return
AFTER UPDATE ON TRANSACTIONS
FOR EACH ROW
BEGIN
    -- Check if the transaction status has changed to 'Returned'.
    IF OLD.status <> 'Returned' AND NEW.status = 'Returned' THEN
        -- Increment the available_copies for the corresponding book.
        UPDATE BOOKS
        SET available_copies = available_copies + 1
        WHERE book_id = NEW.book_id;
    END IF;
END $$

DELIMITER ;


-- Section 7: User Management (Create and Grant Privileges)
-- These commands demonstrate how to create new database users and assign them
-- specific privileges for database access control.

-- 7.1 Create Users
-- Create a 'librarian' user that can connect from 'localhost' with a specified password.
CREATE USER 'librarian'@'localhost' IDENTIFIED BY 'lib_password123';
-- Create a 'student_user' that can connect from 'localhost' with a specified password.
CREATE USER 'student_user'@'localhost' IDENTIFIED BY 'student_password123';


-- 7.2 Grant Privileges
-- Grant all privileges on the 'Library_Management_System' database to the 'librarian' user.
GRANT ALL PRIVILEGES ON Library_Management_System.* TO 'librarian'@'localhost';

-- Grant only SELECT privilege on the 'BOOKS' table within 'Library_Management_System'
-- to the 'student_user'. This means students can only view book information.
GRANT SELECT ON Library_Management_System.BOOKS TO 'student_user'@'localhost';

-- Apply the changes made to the user privileges.
FLUSH PRIVILEGES;


-- Section 8: User Management (Drop Users - for cleanup/re-creation)
-- These commands are typically used for cleaning up or re-initializing user accounts.

-- Drop the 'librarian' user.
DROP USER 'librarian'@'localhost';
-- Drop the 'student_user'.
DROP USER 'student_user'@'localhost';


-- Section 9: Re-creating Users and Granting Privileges (Example)
-- This section provides a consolidated example of creating users and granting
-- their respective privileges, useful for initial setup or re-configuration.

-- Create the 'librarian' user.
CREATE USER 'librarian'@'localhost' IDENTIFIED BY 'lib_password123';
-- Create the 'student_user'.
CREATE USER 'student_user'@'localhost' IDENTIFIED BY 'student_password123';

-- Grant all privileges to the 'librarian' user.
GRANT ALL PRIVILEGES ON Library_Management_System.* TO 'librarian'@'localhost';
-- Grant SELECT privilege on the 'BOOKS' table to the 'student_user'.
GRANT SELECT ON Library_Management_System.BOOKS TO 'student_user'@'localhost';
-- Apply changes.
FLUSH PRIVILEGES;


-- Section 10: Data Analysis Queries (Task 2.1)
-- These queries provide insights into library data, such as book availability,
-- overdue books, and popular titles.

-- 1. List all books with their available copies where available copies are less than total copies
-- This query identifies books that have been borrowed at least once.
SELECT book_id, title, total_copies, available_copies
FROM BOOKS
WHERE available_copies < total_copies;

-- 2. Find members who have overdue books
-- This query lists members who currently have books that are past their due date.
SELECT M.member_id, M.first_name, M.last_name, T.book_id, T.due_date
FROM MEMBERS M
JOIN TRANSACTIONS T ON M.member_id = T.member_id
WHERE T.due_date < CURDATE() AND T.status = 'Overdue';

-- 3. Display the top 5 most borrowed books with their borrow count
-- This query identifies the most popular books based on the number of transactions.
SELECT B.book_id, B.title, COUNT(T.transaction_id) AS borrow_count
FROM BOOKS B
JOIN TRANSACTIONS T ON B.book_id = T.book_id
GROUP BY B.book_id, B.title
ORDER BY borrow_count DESC
LIMIT 5;

-- 4. Show members who have never returned a book on time
-- This query lists members who have at least one transaction where the return date
-- was after the due date.
SELECT DISTINCT M.member_id, M.first_name, M.last_name
FROM MEMBERS M
JOIN TRANSACTIONS T ON M.member_id = T.member_id
WHERE T.return_date > T.due_date;


-- Section 11: Data Manipulation Queries (Task 2.2)
-- These queries demonstrate how to update, insert, and archive data in the database.

-- 1. Update fine amounts for all overdue transactions (₹5 per day after due date)
-- This command sets the SQL_SAFE_UPDATES option to 0, allowing UPDATE/DELETE statements
-- without a WHERE clause that uses a key. Use with caution.
SET SQL_SAFE_UPDATES = 0;

-- This query updates the fine_amount for transactions where the return date is
-- after the due date, calculating a fine of ₹5 per overdue day.
UPDATE TRANSACTIONS
SET fine_amount = DATEDIFF(return_date, due_date) * 5
WHERE return_date > due_date;

-- 2. Insert a new member with membership validation (no duplicate email or phone)
-- This query inserts a new member only if their email and phone number do not
-- already exist in the MEMBERS table, preventing duplicates.
INSERT INTO MEMBERS (member_id, first_name, last_name, email, phone, address, membership_date, membership_type)
SELECT 16, 'Rafi', 'Hasan', 'rafi@example.com', '01234567906', 'Narsingdi, BD', CURDATE(), 'Student'
WHERE NOT EXISTS (
    SELECT 1 FROM MEMBERS WHERE email = 'rafi@example.com' OR phone = '01234567906'
);

-- 3. Archive completed transactions older than 2 years to a separate table
-- This process involves creating an archive table and then moving old, completed
-- transactions from the main TRANSACTIONS table to the archive table.

-- Step 1: Create archive table
-- This command creates a new table 'TRANSACTION_ARCHIVE' with the same structure
-- as 'TRANSACTIONS', but initially empty (WHERE 1=0).
CREATE TABLE IF NOT EXISTS TRANSACTION_ARCHIVE AS
SELECT * FROM TRANSACTIONS WHERE 1 = 0;

-- Step 2: Insert into archive and delete from main table
-- This query inserts transactions that are 'Returned' and older than 2 years
-- into the archive table.
INSERT INTO TRANSACTION_ARCHIVE
SELECT * FROM TRANSACTIONS
WHERE status = 'Returned' AND return_date < CURDATE() - INTERVAL 2 YEAR;

-- This query deletes the archived transactions from the main TRANSACTIONS table.
DELETE FROM TRANSACTIONS
WHERE status = 'Returned' AND return_date < CURDATE() - INTERVAL 2 YEAR;

-- 4. Update book categories based on publication year ranges
-- This query updates the 'category' of books based on their publication year.
UPDATE BOOKS
SET category = CASE
    WHEN YEAR(publication_year) < 2000 THEN 'Classic'
    WHEN YEAR(publication_year) BETWEEN 2000 AND 2010 THEN 'Standard'
    ELSE 'Modern'
END;


-- Section 12: Join Operations (Task 3.1)
-- These queries demonstrate different types of SQL JOINs to combine data from
-- multiple tables.

-- 1. Display transaction history with member details and book information for all overdue books using INNER JOIN
-- This query combines data from TRANSACTIONS, MEMBERS, and BOOKS tables
-- to show detailed information for all overdue transactions.
SELECT
    T.transaction_id,
    M.member_id, M.first_name, M.last_name,
    B.book_id, B.title, B.category,
    T.issue_date, T.due_date, T.return_date, T.status
FROM TRANSACTIONS T
INNER JOIN MEMBERS M ON T.member_id = M.member_id
INNER JOIN BOOKS B ON T.book_id = B.book_id
WHERE T.status = 'Overdue';

-- 2. Show all books and their transaction count (including books never borrowed) using LEFT JOIN
-- This query uses a LEFT JOIN to ensure all books are listed, even if they have
-- no associated transactions, showing a count of 0 for never-borrowed books.
SELECT
    B.book_id, B.title, COUNT(T.transaction_id) AS transaction_count
FROM BOOKS B
LEFT JOIN TRANSACTIONS T ON B.book_id = T.book_id
GROUP BY B.book_id, B.title
ORDER BY transaction_count DESC;

-- 3. Find members who have borrowed books from the same category as other members using SELF JOIN
-- This query uses self-joins on TRANSACTIONS, MEMBERS, and BOOKS to find pairs
-- of distinct members who have borrowed books belonging to the same category.
SELECT DISTINCT
    M1.member_id AS member1_id, M1.first_name AS member1_name,
    M2.member_id AS member2_id, M2.first_name AS member2_name,
    B1.category
FROM TRANSACTIONS T1
JOIN MEMBERS M1 ON T1.member_id = M1.member_id
JOIN BOOKS B1 ON T1.book_id = B1.book_id
JOIN TRANSACTIONS T2 ON B1.category = (
    SELECT B2.category FROM BOOKS B2 WHERE B2.book_id = T2.book_id
)
JOIN MEMBERS M2 ON T2.member_id = M2.member_id
WHERE M1.member_id <> M2.member_id;

-- 4. List all possible member-book combinations for recommendation system using CROSS JOIN (limit to 50 results)
-- This query generates every possible combination of members and books, which can be
-- useful for building a recommendation system, though it's limited to 50 results
-- for practical purposes given its potentially large output.
SELECT
    M.member_id, M.first_name, M.last_name,
    B.book_id, B.title, B.category
FROM MEMBERS M
CROSS JOIN BOOKS B
LIMIT 50;


-- Section 13: Subqueries & Advanced Filtering (Task 3.2)
-- These queries demonstrate the use of subqueries for complex filtering and data retrieval.

-- 1. Find all books that have been borrowed more times than the average borrowing rate across all books
-- This query uses nested subqueries to first calculate the borrowing count for each book,
-- then the average borrowing count across all books, and finally selects books
-- that exceed this average.
SELECT book_id, title
FROM BOOKS
WHERE book_id IN (
    SELECT book_id
    FROM TRANSACTIONS
    GROUP BY book_id
    HAVING COUNT(*) > (
        SELECT AVG(borrow_count)
        FROM (
            SELECT COUNT(*) AS borrow_count
            FROM TRANSACTIONS
            GROUP BY book_id
        ) AS borrow_stats
    )
);

-- 2. List members whose total fine amount is greater than the average fine paid by their membership type
-- This query uses subqueries to calculate the total fine for each member and then
-- compares it to the average fine for their specific membership type.
SELECT member_id, first_name, last_name, total_fine
FROM (
    SELECT M.member_id, M.first_name, M.last_name, M.membership_type,
            SUM(T.fine_amount) AS total_fine
    FROM MEMBERS M
    JOIN TRANSACTIONS T ON M.member_id = T.member_id
    GROUP BY M.member_id, M.first_name, M.last_name, M.membership_type
) AS member_fines
WHERE total_fine > (
    SELECT AVG(total_fine)
    FROM (
        SELECT M.member_id, M.membership_type, SUM(T.fine_amount) AS total_fine
        FROM MEMBERS M
        JOIN TRANSACTIONS T ON M.member_id = T.member_id
        GROUP BY M.member_id, M.membership_type
    ) AS type_fines
    WHERE type_fines.membership_type = member_fines.membership_type
);

-- 3. Display books that are currently available but belong to the same category as the most borrowed book
-- This query identifies available books that share the same category as the book
-- that has been borrowed the most times overall.
SELECT book_id, title, category
FROM BOOKS
WHERE available_copies > 0
AND category = (
    SELECT B.category
    FROM BOOKS B
    JOIN TRANSACTIONS T ON B.book_id = T.book_id
    GROUP BY B.category
    ORDER BY COUNT(*) DESC
    LIMIT 1
);

-- 4. Find the second most active member (by transaction count)
-- This query uses multiple nested subqueries to determine the second highest
-- transaction count and then retrieves the member(s) associated with that count.
SELECT member_id, first_name, last_name, txn_count
FROM (
    SELECT M.member_id, M.first_name, M.last_name, COUNT(T.transaction_id) AS txn_count
    FROM MEMBERS M
    JOIN TRANSACTIONS T ON M.member_id = T.member_id
    GROUP BY M.member_id, M.first_name, M.last_name
) AS member_txns
WHERE txn_count = (
    -- Find the second highest transaction count
    SELECT MAX(txn_count) FROM (
        SELECT COUNT(transaction_id) AS txn_count
        FROM TRANSACTIONS
        GROUP BY member_id
        HAVING COUNT(transaction_id) < (
            -- Highest transaction count
            SELECT MAX(txn_count_inner) FROM (
                SELECT COUNT(transaction_id) AS txn_count_inner
                FROM TRANSACTIONS
                GROUP BY member_id
            ) AS all_txns_inner
        )
    ) AS second_max
);


-- Section 14: Aggregate & Window Functions (Task 3.3)
-- These queries demonstrate the use of aggregate functions and window functions
-- for advanced data analysis, including running totals and ranking.

-- 1. Calculate running total of fines collected by month using window functions
-- This query calculates the monthly fine collection and a running total of fines
-- accumulated over time, ordered by month.
SELECT
    DATE_FORMAT(return_date, '%Y-%m') AS month,
    SUM(fine_amount) AS monthly_fine,
    SUM(SUM(fine_amount)) OVER (ORDER BY DATE_FORMAT(return_date, '%Y-%m')) AS running_total
FROM TRANSACTIONS
WHERE fine_amount > 0
GROUP BY month
ORDER BY month;

-- 2. Rank members by their borrowing activity within each membership type
-- This query ranks members based on their borrowing count, with the ranking
-- reset for each distinct membership type.
SELECT
    member_id, first_name, last_name, membership_type, borrow_count,
    RANK() OVER (PARTITION BY membership_type ORDER BY borrow_count DESC) AS rank_within_type
FROM (
    SELECT M.member_id, M.first_name, M.last_name, M.membership_type, COUNT(T.transaction_id) AS borrow_count
    FROM MEMBERS M
    LEFT JOIN TRANSACTIONS T ON M.member_id = T.member_id
    GROUP BY M.member_id, M.first_name, M.last_name, M.membership_type
) AS sub;

-- 3. Find percentage contribution of each book category to total library transactions
-- This query calculates the total transactions for each book category and
-- expresses it as a percentage of the overall total transactions in the library.
SELECT
    B.category,
    COUNT(T.transaction_id) AS category_transaction_count,
    ROUND(100 * COUNT(T.transaction_id) / (SELECT COUNT(*) FROM TRANSACTIONS), 2) AS percentage_contribution
FROM BOOKS B
LEFT JOIN TRANSACTIONS T ON B.book_id = T.book_id
GROUP BY B.category
ORDER BY percentage_contribution DESC;
