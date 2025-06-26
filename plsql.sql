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
